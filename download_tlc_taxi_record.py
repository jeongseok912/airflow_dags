from datetime import datetime
import requests
import boto3
import logging
import aiohttp
import asyncio
import time

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models import Variable


class DBHandler(logging.StreamHandler):
    def __init__(self):
        super().__init__()
        self.hook = MySqlHook.get_hook(conn_id="TLC_TAXI")
        self.conn = self.hook.get_conn()
        self.cursor = self.conn.cursor()

    def emit(self, record):
        if record:
            self.cursor.execute(
                f"INSERT INTO dataset_log (message, created_at) VALUES ('{record.msg}', SYSDATE());")

    def select(self, sql):
        return self.hook.get_records(sql)

    def close(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()


@task
def get_latest_dataset_id():
    db = DBHandler()
    id = db.select("""
        SELECT
            MAX(dataset_id)
        FROM dataset_log
        WHERE logical_date = (SELECT DATE_SUB(CURDATE(), INTERVAL 1 DAY));
        """
                   )[0][0]

    if id is None:
        id = 1

    db.close()

    return id


@task
def make_dynamic_url(num, **context):
    db = DBHandler()
    id = context['ti'].xcom_pull(task_ids='get_latest_dataset_id')
    start = 0
    end = 0
    urls = []

    if id == 1:
        start = 1
        end = num
    else:
        start = id + 1
        end = id + (num - 1)

    result = db.select(
        f"SELECT dataset_link FROM dataset_meta WHERE id BETWEEN {start} AND {end};")

    for row in result:
        urls.append(row[0])

    db.close()

    return urls


@task
def fetch(url):
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    logger = logging.getLogger("dataset")
    logger.setLevel(logging.INFO)

    dbhandler = DBHandler()
    dbhandler.setFormatter(formatter)
    logger.addHandler(dbhandler)

    downup_start = time.time()

    file_name = url.split("/")[-1]

    id = "NA"
    result = dbhandler.select(
        f"SELECT id FROM dataset_meta WHERE dataset_link = '{url}';")

    for row in result:
        id = str(row[0])
        logger.info(f"dataset {id}: {url}")

    # download dataset
    logger.info(f"dataset {id}: download started.")
    download_start = time.time()

    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f"dataset {id}: download failed.")
        raise Exception(f"dataset {id}: download failed.")

    data = response.content

    logger.info(f"dataset {id}: download completed.")
    download_end = time.time()
    download_elpased = int(download_end - download_start)
    logger.info(f"dataset {id}: download {download_elpased}s elapsed.")

    # upload to s3
    aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    bucket = Variable.get("AWS_S3_BUCKET_TLC_TAXI")
    dir = file_name.split("-")[0].split("_")[-1]
    key = f"{dir}/{file_name}"

    logger.info(f"dataset {id}: S3 upload started.")
    upload_start = time.time()

    s3.put_object(Bucket=bucket, Key=key, Body=data)

    logger.info(f"dataset {id}: S3 upload completed.")
    upload_end = time.time()
    upload_elapsed = int(upload_end - upload_start)
    logger.info(f"dataset {id}: upload {upload_elapsed}s elapsed.")

    downup_end = time.time()
    downup_elapsed = int(downup_end - downup_start)
    logger.info(f"dataset {id}: total {downup_elapsed}s elapsed.")

    dbhandler.close()


with DAG(
    'download_tlc_taxi_record',
    start_date=datetime(2022, 2, 6),
    schedule_interval=None,
) as dag:

    get_latest_dataset_id = get_latest_dataset_id()
    get_urls = make_dynamic_url(num=2)

    get_latest_dataset_id >> get_urls

    fetch.expand(url=get_urls)
