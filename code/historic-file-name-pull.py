
import requests
from bs4 import BeautifulSoup
import os
import re

def report_name_download(webpage, source):

    """
    downloads reports of cities with names to match
    
    webpage : "https://academic.udayton.edu/kissock/http/Weather/"
    source  : local - "https://academic.udayton.edu/kissock/http/Weather/citylistUS.htm"
            : international - "https://academic.udayton.edu/kissock/http/Weather/citylistWorld.htm"

    """
    if ('US') in str(source).split("/")[-1]:
            folder = './data/local'
            if not os.path.exists(folder):
                os.makedirs(folder)

    else:
        folder = './data/international'
        if not os.path.exists(folder):
            os.makedirs(folder)

    source_data = requests.get(source)
    data_res = source_data.text
    soup = BeautifulSoup(data_res)

    cities, cleaned_cities = [], []
    
    # finding text betweeen <b> tags in soup text as it has city names embedded

    for b in soup.find_all("b"):
        if re.search(r'>([^<]*)<', str(b)).group(0)[1:-1]:
            cities.append(re.search(r'>([^<]*)<', str(b)).group(0)[1:-1].strip())
    
    # cleaning city names with extra spaces & unwanted characters
    for city in set(cities):
        if ('(' in city or ')' in city) and len(city)>3:
            new_city = city.replace('(', '').replace(')','').strip()
            cleaned_cities.append(new_city)
        elif len(city)>2 and 'Return' not in city and city != " ":
            cleaned_cities.append(city)
    
    if ('US') in str(source).split("/")[-1]:
        with open('./data/local/00_local_cities.txt', 'w') as f:
            for city in cleaned_cities:
                f.write(f'{city}\n')
    else:
        with open('./data/international/00_international_cities.txt', 'w') as f:
            for city in cleaned_cities:
                f.write(f'{city}\n')


urls = ["https://academic.udayton.edu/kissock/http/Weather/citylistUS.htm",
        "https://academic.udayton.edu/kissock/http/Weather/citylistWorld.htm"]

webpage = "https://academic.udayton.edu/kissock/http/Weather/"


for url in urls:
    report_name_download(
        webpage = webpage,
        source  = url)

if __name__ == "__main__":
    report_name_download()


