from datetime import datetime
# import pymysql
import requests
import boto3

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

'''
def get_meta_log():
    conn = pymysql.connect(
        host='172.23.138.8',
        user='bradley',
        password='123qwe!@#QWE',
        db='tlc_taxi'
    )

    cur = conn.cursor()

    sql = 'SELECT * FROM dataset_meta;'
    cur.execute(sql)
    result = cur.fetchall()
    return result
'''


def download_and_upload_s3(year, month, day, hour, minute, utc_dt, utc_hour, utc_minute, **context):
    print("----------------------------")
    # get next index's dataset link of lasted index
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2019-02.parquet"
    file_name = url.split("/")[-1]

    # download dataset
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"다운로드 실패: {file_name}")
    file_content = response.content

    # logging

    # upload to s3
    s3 = boto3.client("s3")
    bucket = "tlc_taxi"
    key = file_name

    s3.put_objet(bucket, key, file_content)

    # upload_file if you want a simple API or you are uploading large files (>5GB) to your S3 bucket.
    # put_object if you need additional configurability like setting the ACL on the uploaded object
    # s3.upload_file(response, bucket, key)
    '''
    print(context['logical_date'].strftime('%Y-%m-%d'))
    ts = context["logical_date"].timestamp()
    print(int(ts))

    year_month = []
    for i in [2019, 2020, 2021, 2022]:
        for j in range(1, 13):
            month = f"0{j}"[-2:]
            year_month.append(f"{i}-{month}")

    # mongodb metafh 변경
    year_month = year_month[1:]  # 2019년 2월부터 FHVHV 데이터 존재

    # print("******************************************")
    # print("selected dataset: ", get_meta_log())

    i = 0  # DB에 max(created_at)의 index 값 가져오기
    print(year_month[i])
    print(context["run_id"])

    # 1675745379 # 4의 ts 근처 배수
    '''
    print("----------------------------")


with DAG(
    'download_tlc_taxi_record',
    start_date=datetime(2022, 2, 6),
    schedule_interval=None,
) as dag:

    t1 = MySqlOperator(
        task_id='select_dataset_meta',
        mysql_conn_id='TLC_TAXI',
        sql="SELECT * FROM dataset_meta;"
    )

    t2 = PythonOperator(
        task_id="download_dataset",
        python_callable=download_and_upload_s3,
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

t1 >> t2
