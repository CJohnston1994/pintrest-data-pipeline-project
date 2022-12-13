from airflow import DAG
from datetime import datetime, timedelta

with DAG(
    dag_id="pintrest_dag",
    start_date=datetime(2022, 12, 13)
    schedule_interval="* * * * * *",
    catchup=False,
    ) as dag:
    
