from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import airflow_api as api
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook

def city_names():
    with open("./metadata/01_local_cities_name_fix.txt", 'r') as f:
        cities = f.read().splitlines()
        cities = [city.replace(" ","").upper() for city in cities]
    return cities

bash_command = """
                cd /opt/airflow/
                file_path="{{ ti.xcom_pull(key='folder_path' )}}"
                if [ ! -f $file_path/merged_file.json ]
                then
                    for f in $file_path/*.json; do (cat "${f}"; echo) >> $file_path/merged_file.json; done
                    merged_file=$file_path/merged_file.json
                    readlink -f $merged_file
                else
                    echo *** file EXISTS. SKIPPING. ***
                fi
                """

sql_command = """
                DROP TABLE IF EXISTS weather_temp;
                CREATE UNLOGGED TABLE weather_temp (doc JSON);
                COPY weather_temp from '{}' ;
                INSERT INTO weather_daily_local (queryCost, latitude, longitude, resolvedAddress, address, timezone, tzoffset, datetime, datetimeEpoch, tempmax, tempmin, feelslikemax, feelslikemin, feelslike, dew, humidity, precip, precipprob, precipcover, preciptype, snow, snowdepth, windgust, windspeed, winddir, pressure, cloudcover, visibility, solarradiation, solarenergy, uvindex, severerisk, sunrise, sunriseEpoch, sunset, sunsetEpoch, moonphase, conditions, description, icon, station)
                SELECT (doc->>'queryCost')::INT AS queryCost, (doc->>'latitude') AS latitude, (doc->>'longitude') AS longitude,	(doc->>'resolvedAddress') AS resolvedAddress, (doc->>'address') AS "address", (doc->>'timezone') AS timezone, (doc->>'tzoffset')::DOUBLE PRECISION AS tzoffset, (doc->'days'->0->>'datetime')::DATE AS "datetime", (doc->'days'->0->>'datetimeEpoch')::NUMERIC  AS datetimeEpoch, (doc->'days'->0->>'tempmax')::DOUBLE PRECISION AS tempmax, (doc->'days'->0->>'tempmin')::DOUBLE PRECISION AS tempmin, (doc->'days'->0->>'feelslikemax')::DOUBLE PRECISION AS feelslikemax, (doc->'days'->0->>'feelslikemin')::DOUBLE PRECISION AS feelslikemin, (doc->'days'->0->>'feelslike')::DOUBLE PRECISION AS feelslike, (doc->'days'->0->>'dew')::DOUBLE PRECISION AS dew, (doc->'days'->0->>'humidity')::DOUBLE PRECISION AS humidity, (doc->'days'->0->>'precip')::DOUBLE PRECISION AS precip, (doc->'days'->0->>'precipprob')::DOUBLE PRECISION AS precipprob, (doc->'days'->0->>'precipcover')::DOUBLE PRECISION AS precipcover, (doc->'days'->0->>'preciptype')::TEXT AS preciptype, (doc->'days'->0->>'snow')::DOUBLE PRECISION AS snow, (doc->'days'->0->>'snowdepth')::DOUBLE PRECISION AS snowdepth, (doc->'days'->0->>'windgust')::DOUBLE PRECISION AS windgust, (doc->'days'->0->>'windspeed')::DOUBLE PRECISION AS windspeed, (doc->'days'->0->>'winddir')::DOUBLE PRECISION AS winddir, (doc->'days'->0->>'pressure')::DOUBLE PRECISION AS pressure, (doc->'days'->0->>'cloudcover')::DOUBLE PRECISION AS cloudcover, (doc->'days'->0->>'visibility')::DOUBLE PRECISION AS visibility, (doc->'days'->0->>'solarradiation')::DOUBLE PRECISION AS solarradiation, (doc->'days'->0->>'solarenergy')::DOUBLE PRECISION AS solarenergy, (doc->'days'->0->>'uvindex')::DOUBLE PRECISION AS uvindex, (doc->'days'->0->>'severerisk')::DOUBLE PRECISION AS severerisk, (doc->'days'->0->>'sunrise') AS sunrise, (doc->'days'->0->>'sunriseEpoch')::NUMERIC AS sunriseEpoch, (doc->'days'->0->>'sunset') AS sunset, (doc->'days'->0->>'sunsetEpoch')::NUMERIC AS sunsetEpoch, (doc->'days'->0->>'moonphase')::DOUBLE PRECISION AS moonphase, (doc->'days'->0->>'conditions') AS conditions, (doc->'days'->0->>'description') AS description, (doc->'days'->0->>'icon') AS icon, (doc->'days'->'stations')::JSON as stations FROM weather_temp;
                DROP TABLE IF EXISTS weather_temp;
              """.format("{{ ti.xcom_pull(task_ids='combine_files') }}")

default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2022,10,22),
    'schedule_interval' : "@daily",
    'catchup' : False,
    'max_active_runs' : 10,
    'email' : ['akshay.kamath.14@gmail.com'], # <- TO DO
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0    
}

with DAG("scheduled_api_pull_dag", default_args=default_args ) as dag:

    task1 = DummyOperator(task_id='start')

    task2 = PostgresOperator(
        task_id = "create_table_task",
        postgres_conn_id = "postgres_localhost",
        sql = "sql/create_table.sql"
        )

    with TaskGroup('report_pulls', prefix_group_id=False ) as taskgroup1:
        for city in city_names():
            task3 = PythonOperator(
                task_id = 'city_report_pull_{}'.format(city),
                python_callable = api.data_pull,
                op_args = ["local", city],
                provide_context = True,
                do_xcom_push = True,
                trigger_rule = TriggerRule.ALL_DONE
                )
            task3

    task4 = BashOperator(
        task_id = "combine_files",
        bash_command = bash_command,
        do_xcom_push = True,
        trigger_rule = TriggerRule.ALL_DONE
    )

    task5 = PostgresOperator(
        task_id = "push_to_db",
        sql = sql_command,
        postgres_conn_id = "postgres_localhost"
    )

    task6 = DummyOperator(task_id='end')

    task1 >> task2 >> taskgroup1 >> task4 >> task5 >> task6