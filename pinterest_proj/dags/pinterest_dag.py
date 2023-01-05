from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'Clark',
    'retries': 0,
}

with DAG(
    dag_id='pinterest_dag',
    default_args=args,
    start_date=days_ago(1),
    schedule=("5 * * * * *"),
    catchup=False
    ) as dag:

    def clean_data():
        from spark_file import SparkBatchController 
        batch_processor = SparkBatchController()
        batch_processor.run_batch_cleaner()
        
    #task1
    batch_process = PythonOperator(
        task_id='batch_process_task',
        python_callable=clean_data(),
        dag = dag
    )