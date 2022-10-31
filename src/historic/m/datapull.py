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
QUERY="select * from weather.dim_location order by address desc"

engine = create_engine(CONN)

with engine.connect() as CONNECTION:
    df = pd.read_sql(QUERY, CONNECTION)
    print(df.head())


# start = datetime(2022, 10, 15)
# end = datetime(2022, 10, 24)

# city = Point(lat, lon)
# data = Daily(city, start, end)
# data = data.fetch()


