import json
import streamlit as st
from kafka import KafkaConsumer
        
consumer = KafkaConsumer("data",
                         bootstrap_servers=["kafka:9093"],
                         api_version=(2,0,2)
                         )

for msg in consumer:
     st.write(json.loads(msg.value))
    # Sample response 
    # {'meta': 
    #      {'symbol': 'XAU/USD',
    #       'interval': '5min',
    #       'currency_base': 'Gold Spot',
    #       'currency_quote': 'US Dollar',
    #       'type': 'Physical Currency'},
    #      'values': [{'datetime': '2022-10-25 01:30:00',
    #                  'open': '1647.67004',
    #                  'high': '1648.88000',
    #                  'low': '1646.93994',
    #                  'close': '1648.68005'
    #                  }], 
    #   'status': 'ok'}
    
    