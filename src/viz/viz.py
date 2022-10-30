from dash import Dash, html, dcc
from dash.dependencies import Input, Output, State
from dash.dash_table.Format import Group
from dash import dcc
from dash import html

import pandas as pd
import plotly.express as px

from sqlalchemy.sql import select
from sqlalchemy import create_engine

from dotenv import load_dotenv
import os

load_dotenv()

USERNAME=os.getenv('USERNAME')
PASSWORD=os.getenv('PASSWORD')
HOST=os.getenv('HOST')
DATABASE=os.getenv('DATABASE')

CONN='postgresql+psycopg2://{}:{}@{}/{}'.format(USERNAME, PASSWORD, HOST, DATABASE)
QUERY="select * from weather.v_daily_avg_temp where city='{}' ORDER BY datetime;".format('Las Vegas')

engine = create_engine(CONN)

with engine.connect() as CONNECTION:
    df = pd.read_sql(QUERY, CONNECTION)
    print(df.head())
