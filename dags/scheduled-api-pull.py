from nis import cat
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

#from code.daily.api_data_pull import city_report_pull

with open('data/metadata/00_local_cities.txt', 'r') as f:
    cities = f.read().splitlines()

cities_A, cities_B, cities_C = cities[:51],cities[51:102], cities[102:]

print(len(cities_A), len(cities_B), len(cities_C))

# def scheduled_script():
#     city_report_pull()

# with DAG("scheduled_api_pull_dag", start_date=datetime(2022,10,3), schedule_interval="@daily", catchup=False) as dag:
    
#     scheduled_script=PythonOperator(
#         task_id='scheduled_script',
#         python_callable=scheduled_script
#     )