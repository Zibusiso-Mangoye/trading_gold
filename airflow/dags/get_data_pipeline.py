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
            'url': os.getenv('BASE_URL_TWELVE_DATA'),
            'apikey': os.getenv('APIKEY_TWELVE_DATA'),
            'symbol': os.getenv('SYMBOL'),
            'interval': os.getenv('INTERVAL'),
            'outputsize': os.getenv('OUTPUT_SIZE')
            }
        return requests.get(url=PARAMS['url'], params=PARAMS).json()
    
    @task()
    def publish_to_kafka_topic(topic, data) -> None:
        
        import json
        
        from kafka import KafkaProducer
        
        def json_serializer(data):
            return json.dumps(data).encode("utf-8")
        
        producer = KafkaProducer(bootstrap_servers=['kafka:9093'], api_version=(2,0,2) ,value_serializer=json_serializer)
        producer.send(topic, data)
    
    data = get_data_from_twelvedata_api()
    publish_to_kafka_topic("market_data", data)
    
dag = extract_data_from_twelvedata_api()
    