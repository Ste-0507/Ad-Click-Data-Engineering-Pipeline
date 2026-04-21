from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'ad_click_automation_v1',
    start_date=datetime(2026, 4, 21),
    schedule_interval=None,
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id='start',
        bash_command='echo "Pipeline Started"'
    )
    t2 = BashOperator(
        task_id='check_data',
        bash_command='ls ~/ad_click_project/data'
    )
    t1 >> t2
