import pendulum
from airflow.decorators import dag, task

@dag(
    schedule_interval="35 3 * * 5",
    start_date=pendulum.datetime(2022, 11, 3, tz="UTC"),
    catchup=False,
)
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
    def filter_raw_cot_data(raw_data_dict) -> dict:
        
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
    def get_variable_names_from_cftc_website() -> list:
        import requests
        from bs4 import BeautifulSoup

        URL = "https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalViewable/cotvariableslegacy.html"
        r = requests.get(URL)

        soup = BeautifulSoup(r.content, 'lxml')

        v_names = [p.text for p in soup.select("table p")]
        variable_names =[]

        for name in v_names[1:]:
            if " " not in name[:3]:
                variable_names.append(name[4:].replace(" ", "_").strip())
            elif " " in name[:2]: 
                variable_names.append(name[2:].replace(" ", "_").strip())
            else:
                variable_names.append(name[3:].replace(" ", "_").strip())

        variable_names = [i for i in variable_names if i]
        
        return variable_names
        
    @task()  
    def create_cot_dict(variable_names: list , raw_data: list):
        return dict(zip(variable_names, raw_data))
    
    @task()
    def publish_to_kafka_topic(topic, data) -> None:
        
        import json
        
        from kafka import KafkaProducer
        
        def json_serializer(data):
            return json.dumps(data).encode("utf-8")
        
        producer = KafkaProducer(bootstrap_servers=['kafka:9093'], api_version=(2,0,2) ,value_serializer=json_serializer)
        producer.send(topic, data)


    var_names = get_variable_names_from_cftc_website()
    cot_data = get_data_from_cftc_website()

    cot_raw_data_dict = create_cot_dict(var_names, cot_data)

    processed_data = filter_raw_cot_data(cot_raw_data_dict)
    publish_to_kafka_topic("cot_data", processed_data)
    
dag = get_cot_data()