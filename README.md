# Weather Data Pipeline

A project that involves building an automated data pipeline process to pull daily weather data from multiple sources, transform it & then visualize it.

## Pipeline

![pipeline](https://user-images.githubusercontent.com/32349457/203732202-607e0f8e-05b1-41c7-ae47-bba9dec8f0ad.png)

A one time historical data pull (2000-2022) was done using Meteostat. Daily data is sourced from the Visualcrossing API. A scheduled Airflow DAG hosted in a Docker container pulls data for around 150 US cities, transforms it & then loads it into a PostgreSQL database. Using the built-in PostgreSQL plugin, data is then visualized in Grafana.
