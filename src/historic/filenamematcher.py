import json
import os

def file_renamer(location):

    """
    renames files based on country/ state & city in both local & international folders
    
    """

    if location == "local":
        with open('../../data/metadata/us_state_abbr', 'r') as f:
            abbr = json.load(f)[0]
        with open('../../data/local/00_local_cities.txt', 'r') as f:
            cities = f.read().splitlines()

    else:
        with open('../../data/metadata/country_abbr', 'r') as f:
            abbr = json.load(f)[0]
        with open('../../data/international/00_international_cities.txt', 'r') as f:
            cities = f.read().splitlines()

    for k, v in abbr.items():
        if ' ' in v:
            abbr[k] = v.replace(" ", "_")

    cities = [city.replace(' ','').upper() for city in cities]

    filenames,filename_d = [], {}

    new_city = []

    # listing files in dir
    for file_ in os.listdir(f'data/{location}'):
        if not str(file_).startswith('0'):
            filename = os.fsdecode(file_)
            filenames.append(filename)

    # replacing state abbreviations with full names
    for file_ in filenames:
        if file_[:2] in abbr.keys():
            new_state = str(file_).replace(file_[:2], abbr.get(file_[:2]) + "_", 1)
            filename_d[file_] = new_state

    # replacing city abbreviations with full names
    for k, v in filename_d.items():
        city_name = v.split("_")[-1].split(".")[0]
        if any(city_name in s for s in cities):
        #if list(filter(lambda x: x.startswith(city_name), cities)):
            state = '_'.join(v.split("_")[:-1])
            new_city = list(filter(lambda x: x.startswith(city_name), cities))[0].title()
            new_file = '{0}_{1}.txt'.format(state, new_city)
            filename_d[k] = new_file

    # renaming files in dir
    for subdir, dirs, files in os.walk(f'data/{location}'):
        for file_ in files:
            if file_ in filename_d.keys():
                os.rename((os.path.join(subdir, file_)),(os.path.join(subdir, filename_d.get(file_))))

for location in ["local","international"]:
    file_renamer(location=location)