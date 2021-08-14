from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import os

with DAG("coin-extractor",
         start_date=datetime(2021, 8, 13),
         schedule_interval="0 7 * * *") as dag:
    day = datetime.now()
    env = os.environ.copy()
    env.update({
        'GOOGLE_APPLICATION_CREDENTIALS': '/opt/airflow/dags/credentials.json',
        'ds': day.strftime("%Y-%m-%d"),
        'ds_no_dash': day.strftime("%Y%m%d"),
        'ds_coin': day.strftime("%d-%m-%Y"),
    })

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_coins = BashOperator(
        task_id="get_coins",
        bash_command=f'python3 /opt/airflow/dags/scripts/get_coins_price.py',
        env=env,
    )

    # get_coins = PythonOperator(task_id="get_coins",
    #                            python_callable=get_coins_price)

    # get_coins_validator = PythonOperator(task_id="val_get_coins",
    #                                      python_callable=get_coins_price)

    # get_coins >> get_coins_validator

    start >> get_coins >> end
