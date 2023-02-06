from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def _download(**context):
    print("----------------------------")
    print(context)
    print("----------------------------")


with DAG(
    'download_nlt_taxi_record',
    start_date=datetime(2022, 2, 6),
    schedule_interval=None,
) as dag:

    t1 = PythonOperator(
        task_id="download",
        python_callable=_download,
        # op_kwargs={
        #     "execution_date": "{{ execution_date.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
        # }
        provide_context=True
    )

t1
