from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2019-02.parquet

47


def _download(year, month, day, hour, minute, utc_dt, utc_hour, utc_minute, **context):
    print("----------------------------")
    print(context['logical_date'].strftime('%Y-%m-%d'))
    print(int(context["logical_date"].timestamp()))

    year_month = []
    for i in [2019, 2020, 2021, 2022]:
        for j in range(1, 13):
            month = f"0{j}"[-2:]
            year_month.append(f"{i}-{month}")
    year_month = year_month[1:]  # 2019년 2월부터 FHVHV 데이터 존재
    print(len(year_month))
    print("----------------------------")


with DAG(
    'download_nlt_taxi_record',
    start_date=datetime(2022, 2, 6),
    schedule_interval=None,
) as dag:

    t1 = PythonOperator(
        task_id="download",
        python_callable=_download,
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

t1
