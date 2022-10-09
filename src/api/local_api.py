#Downloading weather data using Python as a CSV using the Visual Crossing Weather API
#See https://www.visualcrossing.com/resources/blog/how-to-load-historical-weather-data-using-python-without-scraping/ for more information.

import requests
import os
from datetime import datetime, timedelta
import json
import logging
import sys
from airflow.models import Variable
from dotenv import load_dotenv

def api_data_pull(location_parent:str, location_child:str):

    """
    pull t-1 day's weather report for specified city

    location_parent : local/international
    location_child  : 'New York City'
    """

    try:

        parent, child = location_parent, location_child

       
        # This is the core of our weather query URL
        BaseURL='https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
        
        load_dotenv("../../.env")
        API_KEY = os.getenv('APIKEY')
        
        #UnitGroup sets the units of the output - us or metric
        UnitGroup = 'us'

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
        url = BaseURL + child
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

        folder = (f'./data/02_daily/{parent}/')
        date_folder = folder + StartDate
        file_ = child.replace(" ","").replace(".","").upper()

        try:
            response = requests.get(url).json()
            
            if not os.path.exists(folder):
                os.makedirs(folder)

            if not os.path.exists(date_folder):
                os.makedirs(date_folder)

            if os.path.exists(date_folder):

                if os.path.isfile(date_folder + f'/{file_}.json'):
                    print("File for {0} for {1} already exists. Skipping download.".format(file_, StartDate))
                    pass

                elif not os.path.isfile(date_folder + f'/{file_}.json'):

                    try:
                        with open('{0}/{1}'.format(date_folder, f'/{file_}.json'), "w") as f:
                            json.dump([response], f, indent=4, ensure_ascii=False)
                    
                        logging.basicConfig(filename="../../download_logs",
                                            filemode='a',
                                            format='%(asctime)s %(message)s',
                                            datefmt='%Y-%m-%d %H:%M:%S',
                                            level=logging.DEBUG)

                        logging.info('{0}/{1}'.format(date_folder, file_))
                        print("File for {0} for {1} downloaded!".format(file_, StartDate))
                        print(15*"-")
                        print(os.path.abspath(file_))
                        print(15*"-")
                    except ValueError:
                        print("Unable to fetch report for {0} for {1}. Try again!".format(file_, StartDate))

                    except Exception as e:
                        print(e)          
                        logging.error('{0}/{1}-ERROR:{2}'.format(date_folder, file_, e))  

        except ValueError:
            print("Incorrect Input values. Try again!")                

        except Exception as e:
            print(e)
            logging.error('{0}/{1}-ERROR:{2}'.format(date_folder, file_, e))

    except Exception as e:
        print(e, " - Command Line Arguments Incorrect")

if __name__ == '__main__':    
    try:
        parent, child = sys.argv[1], sys.argv[2]
        api_data_pull(location_parent = parent, location_child = child)
    except Exception as e:
        print("API data pull ERROR - {}".format(e))