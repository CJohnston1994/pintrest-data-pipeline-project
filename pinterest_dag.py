from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Clark",
    "depends_on_past": False,
    "email": ["c.johnston1994@hotmail.co.uk"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "start_date": datetime(2022, 12, 13),
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'trigger_rule': 'all_success'
}

with DAG(dag_id = "test_dag",
        default_args=default_args,
        schedule_interval='*/1 * * * *',
        catchup=False,
        tags=['test']
    ) as dag:
    #define the task.
    test_task = BashOperator(
        task_id='write_data_file',
        bash_command='cd ~/Desktom && date >> ai_core.txt',
        dag=dag)

