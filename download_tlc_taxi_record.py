from datetime import datetime
import requests
import boto3
import logging

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
                f"INSERT INTO log VALUES ('{record.msg}', SYSDATE());")

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
                   )
    print(f"id: {id}")
    if id is None:
        id = 1

    db.close()

    return id


def make_dynamic_url(num, **context):
    db = DBHandler()
    id = context['ti'].xcom_pull(task_ids='get_latest_dataset_id')
    print(f"id: {id}")
    print(f"num: {num}")

    if id == 1:
        db.close()
        return db.select(
            f"SELECT dataset_link FROM dataset_meta WHERE id <= {num};"
        )
    else:
        db.close()
        return db.select(
            f"SELECT dataset_link FROM dataset_meta WHERE id BETWEEN {id + 1} AND {id + (num - 1)};"
        )


def download_dataset_and_upload_to_s3(year, month, day, hour, minute, utc_dt, utc_hour, utc_minute, **context):
    print("----------------------------")
    logger = logging.getLogger("dataset")
    logger.setLevel(logging.INFO)

    dbhandler = DBHandler()
    logger.addHandler(dbhandler)

    # get next index's dataset link of lasted index
    file_name = url.split("/")[-1]
    print(url)
    print("********************************************")

    # download dataset
    logger.info("다운로드 시작: {url}")
    '''
    response = requests.get(url)
    if response.status_code != 200:
        logger.error("다운로드 실패")
        raise Exception(f"다운로드 실패: {url}")
    '''
    logger.info(f"다운로드 완료: {url}")

    # upload to s3
    aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

    s3 = boto3.client("s3", aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)
    bucket = "tlc-taxi"
    dir = file_name.split("-")[0].split("_")[-1]
    key = f"{dir}/{file_name}"

    logger.info("S3 업로드 시작")
    # s3.put_object(Bucket=bucket, Key=key, Body=response.content)
    logger.info("S3 업로드 완료")

    logger.info(url)

    dbhandler.close()


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
    download_dataset_and_upload_to_s3 = PythonOperator(
        task_id="download_dataset_and_upload_to_s3",
        python_callable=download_dataset_and_upload_to_s3,
        op_kwargs={
            "year": "{{ execution_date.in_timezone('Asia/Seoul').year }}",
            "month": "{{ execution_date.in_timezone('Asia/Seoul').month }}",
            "day": "{{ execution_date.in_timezone('Asia/Seoul').day }}",
            "hour": "{{ execution_date.in_timezone('Asia/Seoul').hour }}",
            "minute": "{{ execution_date.in_timezone('Asia/Seoul').minute }}",
            "utc_dt": "{{ execution_date }}",
            "utc_hour": "{{ execution_date.hour }}",
            "utc_minute": "{{ execution_date.minute }}"
        },
        provide_context=True
    )

get_latest_dataset_id >> make_dynamic_url >> download_dataset_and_upload_to_s3
