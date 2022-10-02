#Downloading weather data using Python as a CSV using the Visual Crossing Weather API
#See https://www.visualcrossing.com/resources/blog/how-to-load-historical-weather-data-using-python-without-scraping/ for more information.
import requests
import sys
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

import logging

load_dotenv

source = "local"

with open('./data/metadata/01_local_cities_name_fix.txt', 'r') as f:
    cities = f.read().splitlines()

# This is the core of our weather query URL
BaseURL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
API_KEY=os.environ.get('APIKEY')

#UnitGroup sets the units of the output - us or metric
UnitGroup='us'

#Location for the weather data
Location='New York City'

#Optional start and end dates
#If nothing is specified, the forecast is retrieved. 
#If start date only is specified, a single historical or forecast day will be retrieved
#If both start and and end date are specified, a date range will be retrieved
StartDate   = datetime.strftime(datetime.now() - timedelta(1),'%Y-%m-%d')
EndDate     = datetime.strftime(datetime.now() - timedelta(1),'%Y-%m-%d')
#JSON or CSV 
#JSON format supports daily, hourly, current conditions, weather alerts and events in a single JSON package
#CSV format requires an 'include' parameter below to indicate which table section is required
ContentType="json"

#include sections
#values include days,hours,current,alerts
Include="days"


print('')
print(' - Requesting weather : ')

#basic query including location
url=BaseURL + Location

#append the start and end date if present
if (len(StartDate)):
    url+="/"+StartDate
    if (len(EndDate)):
        url+="/"+EndDate

#Url is completed. Now add query parameters (could be passed as GET or POST)
url+="?"

#append each parameter as necessary
if (len(UnitGroup)):
    url+="&unitGroup="+UnitGroup

if (len(ContentType)):
    url+="&contentType="+ContentType

if (len(Include)):
    url+="&include="+Include

url+="&key="+API_KEY

print(' - Running query URL: ', url)


try:
    response = requests.get(url).json()
    
    if source == "local":
        folder = './data/02_daily/local/'
        if not os.path.exists(folder):
            os.makedirs(folder)

        date_folder = folder + StartDate

        data = []

        if not os.path.exists(date_folder):
            os.makedirs(date_folder)
        elif os.path.exists(date_folder):
            if os.path.isfile(date_folder+"/datadump.json"):
                try:
                    with open('{0}/{1}'.format(date_folder,"datadump.json"), "r", encoding='utf-8') as f:
                        data = json.load(f)
                    
                    data.update(response)
                    
                    with open('{0}/{1}'.format(date_folder,"datadump.json"), "a", encoding='utf-8') as f:
                        json.dump(data, f, indent=4, ensure_ascii=False)

                except Exception as e:
                    print(e)

            elif not os.path.isfile(date_folder+"/datadump.json"):
                try:
                    with open('{0}/{1}'.format(date_folder,"datadump.json"), "w") as f:
                        json.dump(response, f, indent=4, ensure_ascii=False)

                except Exception as e:
                    print(e)            


    elif source == "international":
        folder = './data/02_daily/international'
        if not os.path.exists(folder):
            os.makedirs(folder)

except Exception as e:
    print(e)



            # elif not os.path.isfile(date_folder+"/datadump"):
            #     try:
            #         with open('{0}/{1}'.format(date_folder,"datadump"), "a", encoding='utf-8') as f:
            #             json.dump(response.content.decode('utf8'), f, indent=4)

            #     except Exception as e:
            #         print(e)