from datetime import datetime, timedelta
from textwrap import dedent # text의 모든 줄에서 같은 선행 공백 제거

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failur': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfil',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': <some_function>,
    # 'on_success_callback': <some_other_function>,
    # 'on_retry_callback': <another_function>,
    # 'sla_miss_callback': <yet_another_fucntion>,
    # 'trigger_rule': 'all_success'
}

with DAG(
    'tutorial',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date'
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3
    )
    
    # <task_id>.doc_md : task에 대한 문서 정의
    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )
    
    # <dag_id>.doc_md : dag에 대한 문서 정의
    dag.doc_md = __doc__ 
    dag.doc_md = """
    This is a documentaion placed anywhere
    """

    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )

    t1 >> [t2, t3]