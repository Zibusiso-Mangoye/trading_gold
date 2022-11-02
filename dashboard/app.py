import json
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from kafka import KafkaConsumer

st.set_page_config(
     page_title="Streamlit App",
     page_icon='✌️',
     layout="wide",
     initial_sidebar_state="expanded")


def main():
    df = pd.DataFrame(columns=["Timestamp", "Open", "High", "Low", "Close"])
    market_data_consumer = KafkaConsumer("data",
                            bootstrap_servers=["kafka:9093"],
                            api_version=(2,0,2)
                            )
    market_data_consumer.subscribe()
    
    for msg in market_data_consumer:
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
        tick_data = json.loads(msg.value)["values"][0]
        tick = [tick_data['datetime'], tick_data['open'], tick_data['high'], tick_data['low'], tick_data['close']]
        
        st.write(tick_data)
        df.loc[len(df)] = tick
        
        st.write(df)
        # fig = go.Figure(data=[go.Candlestick(x=df['Timestamp'],
        #                                      open=df['Open'], 
        #                                      high=df['High'],
        #                                      low=df['Low'],
        #                                      close=df['Close']
        #                                      )
        #                       ]
        #                 )

        # fig.update_layout(xaxis_rangeslider_visible=False)
        # fig.show()
        
        
if __name__ == '__main__':
    main()







