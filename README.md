# Weather Data Pipeline

A project that involves building an automated data pipeline process to pull daily weather data from multiple sources, transform it & then visualize it.

## How it works

![pipeline](https://user-images.githubusercontent.com/32349457/203732202-607e0f8e-05b1-41c7-ae47-bba9dec8f0ad.png)

* A one time historical data pull (2000-2022) was done using Meteostat.
* Daily data is sourced from the Visualcrossing API.
* A scheduled Airflow DAG hosted in a Docker container pulls data for around 150 US cities, transforms it & then loads it into a PostgreSQL database.
* Using the built-in PostgreSQL plugin, data is then visualized in Grafana.


## How it looks


![Screen Shot 2022-11-26 at 4 35 40 PM (2)](https://user-images.githubusercontent.com/32349457/204085590-486cfb86-a65c-47a3-b62e-35829bec3491.png)
