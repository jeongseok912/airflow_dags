from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _download(year, month, day, hour, minute, utc_dt, utc_hour, utc_minute, **context):
    print("----------------------------")
    print(year)
    print(month)
    print(day)
    print(hour)
    print(minute)
    print(utc_dt)
    print(utc_hour)
    print(utc_minute)
    print(context['logical_date'].strftime('%Y-%m-%d'))
    print(int(context["logical_date"]))
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
