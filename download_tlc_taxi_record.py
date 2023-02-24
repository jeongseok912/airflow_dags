from datetime import datetime
import requests
import boto3
import logging
import aiohttp
import asyncio
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator, MySqlHook
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


async def fetch_and_upload(url, logger):
    print(f"다운로드 & 업로드 시작 - {url}")
    downup_start = time.time()

    file_name = url.split("/")[-1]

    # download dataset
    logger.info(f"다운로드 시작 - {url}")
    download_start = time.time()

    # response = await loop.run_in_executor(None, requests.get, url)
    # data = await loop.run_in_executor(None, response.content)
    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f"다운로드 실패 - {url}")
        raise Exception(f"다운로드 실패 - {url}")

    data = response.content

    logger.info(f"다운로드 완료 - {url}")
    download_end = time.time()
    download_elpased = int(download_end - download_start)
    print(f"다운로드 시간: {download_elpased}초, {url}")

    # upload to s3
    aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    bucket = Variable.get("AWS_S3_BUCKET_TLC_TAXI")
    dir = file_name.split("-")[0].split("_")[-1]
    key = f"{dir}/{file_name}"

    logger.info(f"S3 업로드 시작 - {url}")
    upload_start = time.time()

    s3.put_object(Bucket=bucket, Key=key, Body=data)

    logger.info(f"S3 업로드 완료 - {url}")
    upload_end = time.time()
    upload_elapsed = int(upload_end - upload_start)
    print(f"업로드 시간: {upload_elapsed}초, {url}")

    downup_end = time.time()
    downup_elapsed = int(downup_end - downup_start)
    print(f"다운로드 & 업로드 완료 - {url}")
    print(f"다운로드 & 업로드 경과시간: {downup_elapsed}초")


async def gather(urls):
    logger = logging.getLogger("dataset")
    logger.setLevel(logging.INFO)

    dbhandler = DBHandler()
    logger.addHandler(dbhandler)

    await asyncio.gather(*[fetch_and_upload(url, logger) for url in urls])

    dbhandler.close()


def async_download_upload(**context):
    urls = context['ti'].xcom_pull(task_ids='make_dynamic_url')
    print("***********************")
    print("***** 이벤트 시작 *****")
    print("***********************")
    start = time.time()

    '''
    loop = asyncio.get_event_loop()
    loop.run_until_complete(gather(urls))
    loop.close()
    '''
    asyncio.run(gather(urls))

    print("***********************")
    print("***** 이벤트 종료 *****")
    print("***********************")
    end = time.time()
    elapsed = int(end - start)
    print(f"이벤트 경과 시간: {elapsed}초")


with DAG(
    'download_tlc_taxi_record',
    start_date=datetime(2022, 2, 6),
    schedule_interval=None,


) as dag:

    get_latest_dataset_id = PythonOperator(
        task_id="get_latest_dataset_id",
        python_callable=get_latest_dataset_id
    )

    make_dynamic_url = PythonOperator(
        task_id="make_dynamic_url",
        python_callable=make_dynamic_url,
        op_kwargs={
            "num": 2
        }
    )
    async_download_upload = PythonOperator(
        task_id="async_download_upload",
        python_callable=async_download_upload,
        provide_context=True
    )

get_latest_dataset_id >> make_dynamic_url >> async_download_upload
