import os
from datetime import datetime

from sqlalchemy import create_engine
from meteostat import Point, Daily
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

USERNAME=os.getenv('USERNAME')
PASSWORD=os.getenv('PASSWORD')
HOST=os.getenv('HOST')
DATABASE=os.getenv('DATABASE')

CONN='postgresql+psycopg2://{}:{}@{}/{}'.format(USERNAME, PASSWORD, HOST, DATABASE)
QUERY="""
        SELECT latitude AS lat, 
                longitude AS lon,
                CASE
                    WHEN resolvedaddress ILIKE '%%Fort Worth Ave%%' THEN 'Fort Worth, TX'
                    WHEN resolvedaddress ILIKE '%%Uptown St. Petersburg%%' THEN 'St Petersburg, FL'
                    WHEN resolvedaddress ILIKE '%%Saint Paul Ave%%' THEN 'Saint Paul Ave, MN'
                    ELSE resolvedaddress
                END AS add
            FROM weather.dim_location ORDER BY 3;
            """

engine = create_engine(CONN)

with engine.connect() as CONNECTION:
    df = pd.read_sql(QUERY, CONNECTION)
    dfl = df.values.tolist()

start = datetime(2020, 5, 14)
end = datetime(2022, 10, 24)

for lat, lon, add in dfl:
    city = Point(eval(lat), eval(lon))
    data = Daily(city, start, end)
    try:
        data = data.fetch()
        data.reset_index(inplace=True)
        data['lat'], data['lon'], data['add']  = lat, lon, add
        city = "_".join(i.strip().replace(" ","") for i in add.split(',')[:2])
        data.to_csv(f'data/01_historic/local/raw/meteostat/{city}.csv', index=False)
    except Exception as e:
        print(f'DATAPULL ERROR for address - {add}. REASON - {e}')