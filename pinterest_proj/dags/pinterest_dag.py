from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import config as c

args = {
    'owner': 'Clark',
    'depends_on_past': False,
    'schedule':'@daily',
    'email': [c.AIRFLOW_EMAIL],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily'
}

with DAG(
    dag_id='pinterest_dag',
    default_args=args,
    start_date=days_ago(1),
    catchup=False
    ) as dag:

        def task1():
            from pinterest_proj.Consumers.batch_processing import SparkBatchController 
            batch_processor = SparkBatchController()
            batch_processor.run_batch_cleaner()
            
        #task1
        batch_process = PythonOperator(
            task_id='batch_process_task',
            python_callable=task1,
            dag = dag
        )