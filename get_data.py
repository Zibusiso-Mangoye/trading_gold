import requests
import os
import configparser
from dotenv import load_dotenv

# loading enviroment variables defined in the .env file
load_dotenv()

# Importing application settings in the settings.ini
config = configparser.ConfigParser()
config.read("settings.ini")

PARAMS = {'apikey': os.getenv('APIKEY'),
          'symbol': config['PARAMS']['SYMBOL'],
          'interval': config['PARAMS']['INTERVAL'],
          'outputsize': config['PARAMS']['OUTPUT_SIZE']
          }

r = requests.get(url = config['PARAMS']['BASE_URL'], params = PARAMS)

data = r.json()
print(data)