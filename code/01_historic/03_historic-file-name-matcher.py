import json
import os

with open('data/metadata/us_state_abbr', 'r') as f:
    abbr = json.load(f)

with open('data/local/00_local_cities.txt', 'r') as f:
    cities = f.read().splitlines()


for f in os.listdir('data/local'):
     filename = os.fsdecode(f)
     