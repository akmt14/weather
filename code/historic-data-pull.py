import requests
from bs4 import BeautifulSoup
import os
import logging

def report_download(webpage, source):

    """
    downloads historic reports
    
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

    filecount = 0

    for link in soup.find_all('a'):
        if str(link.get('href')).endswith(".txt"):
            url = "{0}{1}".format(webpage, link.get('href'))
            try:
                response = requests.get(url)
                filename = link.get("href").split("/")[1]

                with open('{0}/{1}'.format(folder, filename), "w") as a:
                    a.write(response.text)
                
                filecount += 1

                print(f'file {filecount} {filename} downloaded.')

                logging.basicConfig(filename="./logs",
                                    filemode='a',
                                    format='%(asctime)s %(message)s',
                                    datefmt='%Y-%m-%d %H:%M:%S',
                                    level=logging.DEBUG)

                logging.info({filename})

            except Exception as e:
                print(e)
                logging.error({filename})


urls = ["https://academic.udayton.edu/kissock/http/Weather/citylistUS.htm", 
        "https://academic.udayton.edu/kissock/http/Weather/citylistWorld.htm"]

webpage = "https://academic.udayton.edu/kissock/http/Weather/"


for url in urls:
    report_download(webpage = webpage,
        source  = url)

if __name__ == "__main__":
    report_download()

