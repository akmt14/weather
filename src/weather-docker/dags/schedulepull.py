from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime
import api

with open("./metadata/00_local_cities.txt", 'r') as f:
    cities = f.read().splitlines()

n = len(cities)//2
cities_A, cities_B = ([cities[i:i + n] for i in range(0, len(cities), n)])

def api_caller(location_parent:str, location_child:str):
    api.api_data_pull(location_parent, location_child)

with DAG("scheduled_api_pull_dag", start_date=datetime(2022,10,3), schedule_interval="@daily", catchup=False) as dag:
    
    for city in cities_A[:6]:
        city_report_pull=PythonOperator(
        task_id='city_report_pull_{}'.format(city.replace(" ","")),
        python_callable=api_caller,
        op_args=["local",city]
        )

    city_report_pull