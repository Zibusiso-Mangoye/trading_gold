import os
from sqlite3 import Timestamp
import requests
import configparser

import pandas as pd
import numpy as np
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
meta_data = data['meta']
tick_data = data['values']
symbol = meta_data['symbol']
interval = meta_data['interval']

tick_dataframe = pd.DataFrame(columns=['Timestamp', 'Open', 'High', 'Low', 'Close'])

for i in range(int(PARAMS['outputsize'])):
    tick_dataframe.loc[len(tick_dataframe.index)] = [tick_data[i]['datetime'], tick_data[i]['open'], tick_data[i]['high'], tick_data[i]['low'], tick_data[i]['close']]
    
tick_dataframe.to_csv("tick_data.csv", index=False, na_rep='NULL')