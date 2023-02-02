from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
  'GitSyncTestDag',
  start_date = datetime(2022, 2, 3),
  schedule_interval = None,
) as dag:

  t1 = DummyOperator(
    task_id="print_date",
    bash_command='date'
  )
  
t1
