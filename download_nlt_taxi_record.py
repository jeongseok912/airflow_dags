from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _download(year, month, day, hour, minute, full_dt, **context):
    print("----------------------------")
    print(year)
    print(month)
    print(day)
    print(hour)
    print(minute)
    print(full_dt)
    print(context['logical_date'].strftime('%Y-%m-%d'))
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
            "full_dt": "{{ execution_date }}"
        },
        provide_context=True
    )

t1
