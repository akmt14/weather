#Downloading weather data using Python as a CSV using the Visual Crossing Weather API
#See https://www.visualcrossing.com/resources/blog/how-to-load-historical-weather-data-using-python-without-scraping/ for more information.

import os
import sys
from datetime import datetime, timedelta
import json
import requests
import logger as log

from airflow.exceptions import AirflowFailException, AirflowException
from airflow.models import Variable


def data_pull(location_parent:str, location_child:str, s_date:str, ti, **context):

    """
    pull t-1 day's weather report for specified city

    location_parent : local/international
    location_child  : 'New York City'
    s_date
    """

    try:

        parent, child = location_parent, location_child

        # This is the core of our weather query URL
        BaseURL='https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
        
        API_KEY=Variable.get("WeatherAPIKEY")
        
        #UnitGroup sets the units of the output - us or metric
        UnitGroup = 'us'

        #Optional start and end dates
        #If nothing is specified, the forecast is retrieved. 
        #If start date only is specified, a single historical or forecast day will be retrieved
        #If both start and and end date are specified, a date range will be retrieved

        StartDate = datetime.strftime(datetime.strptime(s_date, '%Y-%m-%d') - timedelta(1), '%Y-%m-%d') #datetime.strftime(datetime.now() - timedelta(1),'%Y-%m-%d')
        EndDate = StartDate #datetime.strftime(datetime.now() - timedelta(1),'%Y-%m-%d')
        
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

        parent_folder = f'../data/02_daily/{parent}/'
        _folder = parent_folder + StartDate
        _file = child.replace(" ","").replace(".","").upper()
        logs = "../data/logs/"

        try:
            
            if not os.path.exists(parent_folder):
                os.makedirs(parent_folder)

            if not os.path.exists(_folder):
                os.makedirs(_folder)

            try:
                if not os.path.exists(logs):
                    os.makedirs(logs)                        
            except Exception as e:
                raise AirflowFailException('Unable to create download log folder.', e)

            if os.path.exists(_folder):

                if os.path.isfile(_folder + f'/{_file}.json'):
                    print("File for {0} for {1} already exists. Skipping download.".format(_file, StartDate))
                    pass

                elif not os.path.isfile(_folder + f'/{_file}.json'):

                    try:
                        response = requests.get(url).json()
                        with open('{0}/{1}'.format(_folder, f'/{_file}.json'), "w") as f:
                            json.dump(response, f)
                        lr = log.log_record(_folder, _file)
                        msg = lr.success()
                        lr.write_log(msg)
                        
                        folder_path = _folder
                        ti.xcom_push(key='folder_path', value=folder_path)
                                
                    except Exception as e:
                        lr = log.log_record(_folder, _file, e)
                        msg = lr.fail()
                        lr.write_log(msg)
                        raise AirflowException("Unable to fetch report for {0} for {1}. Reason - {2}!".format(_file, StartDate, e))

        except ValueError as e:
            lr = log.log_record(_folder, _file, e)
            msg = lr.fail()
            lr.write_log(msg)
            raise AirflowException("Unable to fetch report for {0} for {1}. Reason - {2}!".format(_file, StartDate, e))            
        
        except Exception as e:
            lr = log.log_record(_folder, _file, e)
            msg = lr.fail()
            lr.write_log(msg)
            raise AirflowException("Unable to fetch report for {0} for {1}. Reason - {2}!".format(_file, StartDate, e))    

    except Exception as e:
        raise AirflowException("Error Reason - {0}!".format(e))

if __name__ == '__main__':    
    try:
        parent, child, s_Date = sys.argv[1], sys.argv[2], sys.argv[3]
        data_pull(location_parent = parent, location_child = child , s_date = s_date)
    except Exception as e:
        print("API data pull ERROR - {}".format(e))