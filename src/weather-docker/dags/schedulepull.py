from email import message

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import airflow_api as api

with open("./metadata/01_local_cities_name_fix.txt", 'r') as f:
    cities = f.read().splitlines()

with DAG("scheduled_api_pull_dag", start_date=datetime(2022,10,3), schedule_interval="@daily", catchup=False, max_active_runs=10) as dag:

    for city in cities:
        city_report_pull=PythonOperator(
                task_id='city_report_pull_{}'.format(city.replace(" ","")),
                python_callable=api.data_pull,
                op_args=["local", city]
                )

    city_report_pull