#Downloading weather data using Python as a CSV using the Visual Crossing Weather API
#See https://www.visualcrossing.com/resources/blog/how-to-load-historical-weather-data-using-python-without-scraping/ for more information.
from posixpath import sep
import requests
import sys
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json

import logging

load_dotenv


def city_report_pull(location):

    """
    pull t-1 day's weather report for city in cities

    location : city name, e.g. - 'New York City'

    """

    source = "international"

    # This is the core of our weather query URL
    BaseURL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
    API_KEY = os.environ.get('APIKEY')

    #UnitGroup sets the units of the output - us or metric
    UnitGroup = 'us'

    #Location for the weather data
    Location = location

    #Optional start and end dates
    #If nothing is specified, the forecast is retrieved. 
    #If start date only is specified, a single historical or forecast day will be retrieved
    #If both start and and end date are specified, a date range will be retrieved

    StartDate = datetime.strftime(datetime.now() - timedelta(1),'%Y-%m-%d')
    EndDate = datetime.strftime(datetime.now() - timedelta(1),'%Y-%m-%d')
    
    #JSON or CSV 
    #JSON format supports daily, hourly, current conditions, weather alerts and events in a single JSON package
    #CSV format requires an 'include' parameter below to indicate which table section is required
    ContentType = "json"

    #include sections
    #values include days,hours,current,alerts
    Include = "days"

    #basic query including location
    url = BaseURL + Location

    #append the start and end date if present
    if (len(StartDate)):
        url += "/" + StartDate
        if (len(EndDate)):
            url += "/" + EndDate

    #Url is completed. Now add query parameters (could be passed as GET or POST)
    url += "?"

    #append each parameter as necessary
    if (len(UnitGroup)):
        url += "&unitGroup=" + UnitGroup

    if (len(ContentType)):
        url += "&contentType=" + ContentType

    if (len(Include)):
        url += "&include=" + Include

    url += "&key=" + API_KEY

    try:
        response = requests.get(url).json()
        
        folder = (f'./data/02_daily/{source}/')

        if not os.path.exists(folder):
            os.makedirs(folder)

        date_folder = folder + StartDate
        file_ = location.upper()

        if not os.path.exists(date_folder):
            os.makedirs(date_folder)

        if os.path.exists(date_folder):
            if os.path.isfile(date_folder + f'/{file_}.json'):
                pass

            elif not os.path.isfile(date_folder + f'/{file_}.json'):
                try:
                    with open('{0}/{1}'.format(date_folder, f'/{file_}.json'), "w") as f:
                        json.dump([response], f, indent=4, ensure_ascii=False)
                
                    logging.basicConfig(filename="./download_logs",
                                        filemode='a',
                                        format='%(asctime)s %(message)s',
                                        datefmt='%Y-%m-%d %H:%M:%S',
                                        level=logging.DEBUG)

                    logging.info('{0}/{1}'.format(date_folder, file_))

                except Exception as e:
                    print(e)            
                
    except Exception as e:
        print(e)
        logging.error('{0}/{1}'.format(date_folder, file_))

city_report_pull('Tokyo')