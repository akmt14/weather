import requests
import os
from dotenv import load_dotenv
import logging

load_dotenv

API_KEY=os.environ.get('secretKey')
lat = 39.31
lon =  -74.5
part = "hourly,minutely,alerts,current"

# def data_pull(lat, lon, part="hourly,minutely,alerts,current", API_KEY=API_KEY):
#     r=requests.get(f'https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={API_KEY}')
# try:
#     x = data_pull(39.31, -74.5)
#     print(x)
# except Exception as e:
#     print(e)

#r = requests.get('https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={API_KEY}')
print(f'https://api.openweathermap.org/data/3.0/onecall?lat=33.44&lon=-94.04&exclude=hourly,daily&appid={API_KEY}')
