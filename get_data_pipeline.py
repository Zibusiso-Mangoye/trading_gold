import os
import requests
import configparser

import pandas as pd
import numpy as np
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

# loading enviroment variables defined in the .env file
load_dotenv()

# Importing application settings in the settings.ini
config = configparser.ConfigParser()
config.read("settings.ini")

PARAMS = {
    'url': config['PARAMS']['BASE_URL'],
    'apikey': os.getenv('APIKEY'),
    'symbol': config['PARAMS']['SYMBOL'],
    'interval': config['PARAMS']['INTERVAL'],
    'outputsize': config['PARAMS']['OUTPUT_SIZE']
    }

@dag(
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2022, 8, 26, tz="UTC"),
    catchup=False,
)

def tutorial_taskflow_api_etl():
    
    @task(multiple_outputs=True)
    def get_data_from_twelvedata_api(url: str, params: dict) -> dict:
        return requests.get(url=url, params=params).json()
    
    @task(multiple_outputs=True)
    def get_tick_data(data: dict) -> dict:
        return data['values'][0]
    
    @task(multiple_outputs=True)
    def get_meta_data(data: dict) -> dict:
        return data['meta']
    
    @task()
    def publish_to_kafka_topic(data:str) -> None:
        
        import json
        
        from kafka import KafkaProducer
        
        def json_serializer(data):
            return json.dumps(data).encode("utf-8")
        
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)
        producer.send("tick-data", data)
    
    raw_data = get_data_from_twelvedata_api(url, params)
    tick_data = getget_tick_data(raw_data)
    publish_to_kafka_topic(tick_data)
    
    