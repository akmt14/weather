from email import message
from email.policy import default

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import airflow_api as api
from airflow.operators.postgres_operator import PostgresOperator
from sql import create_table

with open("./metadata/01_local_cities_name_fix.txt", 'r') as f:
    cities = f.read().splitlines()

default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2022,10,3),
    'schedule_interval' : "@daily",
    'catchup' : False,
    'max_active_runs' : 10,
    'email' : ['akshay.kamath.14@gmail.com'], # <- TO DO
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0    
}

with DAG("scheduled_api_pull_dag", default_args=default_args) as dag:

    for city in cities:
        city_report_pull=PythonOperator(
                task_id='city_report_pull_{}'.format(city.replace(" ","")),
                python_callable=api.data_pull,
                op_args=["local", city]
                )
    # create_table = PostgresOperator(
    #     sql = create_table_sql_query,
    #     task_id = "create_table_task",
    #     postgres_conn_id = "postgres_local"
    #     )
        

    city_report_pull