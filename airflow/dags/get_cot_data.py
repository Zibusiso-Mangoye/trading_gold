import pendulum
from airflow.decorators import dag, task

@dag(
    schedule_interval="* * * * *",
    start_date=pendulum.datetime(2022, 9, 2, tz="UTC"),
    catchup=False,
)

def _parse_variable_names(filename: str):
    variable_names = []
    with open("variable_names.txt") as file:
        for line in file:
            if " " not in line[:3]:
                variable_names.append(line[4:].replace(" ", "_").strip())
            elif " " in line[:2]: 
                variable_names.append(line[2:].replace(" ", "_").strip())
            else:
                variable_names.append(line[3:].replace(" ", "_").strip())

        variable_names = [i for i in variable_names if i]
        
    return variable_names
    
def get_cot_data():
    
    @task()
    def get_data_from_cftc_website() -> list:
        
        import requests
        
        url = 'https://www.cftc.gov/dea/newcot/deafut.txt'

        data = requests.get(url=url).content.decode("utf-8") 
        data_as_a_list = data.splitlines()

        for i, v in enumerate(data_as_a_list):
            if 'GOLD' in v:
                raw_gold_data = data_as_a_list[i].split(",")
                gold_data = [data.strip() for data in raw_gold_data]  
        
        return gold_data
    @task()
    def filter_raw_cot_data(raw_data):
        variable_names = _parse_variable_names("data/variable_names")        
        raw_data_dict = dict(zip(variable_names, raw_data))
            
        return {
            'Market_and_Exchange_Names' : raw_data_dict['Market_and_Exchange_Names'],
            'Date': raw_data_dict['As_of_Date_in_Form_YYYY-MM-DD'],
            'Noncommercial_Positions-Long' : float(raw_data_dict['Noncommercial_Positions-Long_(All)']),
            'Noncommercial_Positions-Short' : float(raw_data_dict['Noncommercial_Positions-Short_(All)']),
            'Change_in_Noncommercial-Long' : float(raw_data_dict['Change_in_Noncommercial-Long_(All)']),
            'Change_in_Noncommercial-Short' : float(raw_data_dict['Change_in_Noncommercial-Short_(All)']),
            '%_OF_OI-Noncommercial-Long' : float(raw_data_dict['%_OF_OI-Noncommercial-Long_(All)']),
            '%_OF_OI-Noncommercial-Short' : float(raw_data_dict['%_OF_OI-Noncommercial-Short_(All)'])
        }
    
    @task()
    def publish_to_kafka_topic(topic, data) -> None:
        
        import json
        
        from kafka import KafkaProducer
        
        def json_serializer(data):
            return json.dumps(data).encode("utf-8")
        
        producer = KafkaProducer(bootstrap_servers=['kafka:9093'], api_version=(2,0,2) ,value_serializer=json_serializer)
        producer.send(topic, data)
    
    raw_data = get_data_from_cftc_website()
    data = filter_raw_cot_data(raw_data)
    publish_to_kafka_topic("cot_data", data)
    
dag = get_cot_data()