from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import os

default_args = {
    "retries": 4,
    "retry_delay": timedelta(minutes=5),
}

with DAG("coin-extractor",
         default_args=default_args,
         start_date=datetime(2021, 1, 1),
         schedule_interval="0 7 * * *") as dag:
    env = os.environ.copy()
    env.update({
        'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/dags/credentials.json',
    })

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_coins = BashOperator(
        task_id="get_coins",
        bash_command=f'python3 /opt/airflow/dags/scripts/get_coins_price.py',
        env=env,
    )

    get_coins_validator = BashOperator(
        task_id="val_get_coins",
        bash_command=f'python3 /opt/airflow/dags/scripts/validation_get_coins_price.py',
        env=env
    )

    get_coins >> get_coins_validator

    start >> get_coins >> end
