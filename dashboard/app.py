import json
from time import sleep
import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from kafka import KafkaConsumer

st.set_page_config(
     page_title="Streamlit App",
     page_icon='✌️',
     layout="wide",
     initial_sidebar_state="expanded")

def process_market_data(message, df):
    # Sample response 
    # [
        # 0:[
            # 0:"market_data"
            # 1:0
            # 2:91
            # 3:1667403081001
            # 4:0
            # 5:NULL
            # 6:"b'{"meta": {"symbol": "XAU/USD", "interval": "5min", "currency_base": "Gold Spot", "currency_quote": "US Dollar", "type": "Physical Currency"}, "values": [{"datetime": "2022-11-03 02:25:00", "open": "1647.10999", "high": "1647.48999", "low": "1646.97998", "close": "1647.41003"}], "status": "ok"}'"
            # 7:[]
            # 8:NULL
            # 9:-1
            # 10:294
            # 11:-1
        # ]
    # ]
    tick_data = json.loads(message[0][6])["values"][0]
    tick = [tick_data['datetime'], tick_data['open'], tick_data['high'], tick_data['low'], tick_data['close']]
    df.loc[len(df)] = tick
    fig = go.Figure(data=[go.Candlestick(x=df['Timestamp'],
                                            open=df['Open'], 
                                            high=df['High'],
                                            low=df['Low'],
                                            close=df['Close']
                                            )
                        ]
                )

    fig.update_layout(xaxis_rangeslider_visible=False)
    return fig

def process_cot_data(message, df):
    # Sample response
    # [
    # 0:[
        # 0:"cot_data"
        # 1:0
        # 2:64
        # 3:1667403202998
        # 4:0
        # 5:NULL
        # 6:"b'{"Market_and_Exchange_Names": "\\"GOLD - COMMODITY EXCHANGE INC.\\"", "Date": "2022-10-25", "Noncommercial_Positions-Long": 212853.0, "Noncommercial_Positions-Short": 144821.0, "Change_in_Noncommercial-Long": 1963.0, "Change_in_Noncommercial-Short": 10887.0, "%_OF_OI-Noncommercial-Long": 46.7, "%_OF_OI-Noncommercial-Short": 31.8}'"
        # 7:[]
        # 8:NULL
        # 9:-1
        # 10:329
        # 11:-1
        # ]
    # ]
    cot_data = json.loads(message[0][6])
    df.append(cot_data, ignore_index=True)
    return df

@st.cache(allow_output_mutation=True)
def tick_dataframe():
    return pd.DataFrame(columns=["Timestamp", "Open", "High", "Low", "Close"])

@st.cache(allow_output_mutation=True)
def cot_dataframe():
    return pd.DataFrame(columns=["Market_and_Exchange_Names", "Date", "Noncommercial_Positions-Long",
                                   "Noncommercial_Positions-Short", "Change_in_Noncommercial-Long",
                                   "Change_in_Noncommercial-Short", "%_OF_OI-Noncommercial-Long", "%_OF_OI-Noncommercial-Short"])

def main():
    tick_df = tick_dataframe()
    cot_df =  cot_dataframe()
    consumer = KafkaConsumer(bootstrap_servers=["kafka:9093"], api_version=(2,0,2))
    # creating a single-element container
    placeholder = st.empty()
    consumer.subscribe(['market_data', 'cot_data'])
    
    while True:
        # poll messages each certain ms
        raw_messages = consumer.poll(
            timeout_ms=100, max_records=1
        )
        
        with placeholder.container():
            # create two columns for charts
            fig_col1, fig_col2 = st.columns(2)
            
            for topic_partition, messages in raw_messages.items():
                # if message topic is k_connectin_status
                if topic_partition.topic == 'market_data':
                    with fig_col1:
                        st.markdown("### First Chart")
                        fig = process_market_data(messages, tick_df)
                        st.write(fig)
                        st.write(tick_df)
                # if message topic is k_armed_status
                elif topic_partition.topic == 'cot_data':
                    with fig_col2:
                        st.markdown("### Second Chart")
                        fig2 = process_cot_data(messages, cot_df)
                        st.write(fig2)
        # sleep(306)
        
if __name__ == '__main__':
    main()







