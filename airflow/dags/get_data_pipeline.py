import pendulum
from airflow.decorators import dag, task

@dag(
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2022, 9, 2, tz="UTC"),
    catchup=False,
)
def extract_data_from_twelvedata_api():
    
    @task(multiple_outputs=True)
    def get_data_from_twelvedata_api() -> dict:
        
        import os
        import requests

        PARAMS = {
            'url': os.getenv('BASE_URL'),
            'apikey': os.getenv('APIKEY'),
            'symbol': os.getenv('SYMBOL'),
            'interval': os.getenv('INTERVAL'),
            'outputsize': os.getenv('OUTPUT_SIZE')
            }
        
        return requests.get(url=PARAMS['url'], params=PARAMS).json()
    
    @task(multiple_outputs=True)
    def get_tick_data(data: dict) -> dict:
        return data['values'][0]
    
    # @task(multiple_outputs=True)
    # def get_meta_data(data: dict) -> dict:
    #     return data['meta']
    
    # @task()
    # def publish_to_kafka_topic(data:str) -> None:
        
    #     import json
        
    #     from kafka import KafkaProducer
        
    #     def json_serializer(data):
    #         return json.dumps(data).encode("utf-8")
        
    #     producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=json_serializer)
    #     producer.send("tick-data", data)
    
    raw_data = get_data_from_twelvedata_api()
    tick_data = get_tick_data(raw_data)
    print(tick_data)
    
dag = extract_data_from_twelvedata_api()
    