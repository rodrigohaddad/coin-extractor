from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
#from scripts.get_coins_price import get_coins_price

with DAG("coin-extractor",
         start_date=datetime(2021, 8, 13),
         schedule_interval="0 7 * * *") as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    get_coins = BashOperator(
        task_id="get_coins",
        bash_command=f'python3 /opt/airflow/dags/scripts/get_coins_price.py',
        #env=env,
    )

    # get_coins = PythonOperator(task_id="get_coins",
    #                            python_callable=get_coins_price)

    # get_coins_validator = PythonOperator(task_id="get_coins_validator",
    #                                      python_callable=get_coins_price)

    # get_coins >> get_coins_validator

    start >> get_coins >> end
