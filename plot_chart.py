# Importing the necessary charting library
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Importing the Data
my_ohlc_data = pd.read_csv('tick_data.csv')

def ohlc_plot(Data, window, name):
    
    Chosen = Data[-window:, ]
    
    for i in range(len(Chosen)):
        plt.vlines(x = i, ymin = Chosen[i, 2], ymax = Chosen[i, 1], color = 'black', linewidth = 1)
        
        if Chosen[i, 3] > Chosen[i, 0]:
            color_chosen = 'green'
            plt.vlines(x = i, ymin = Chosen[i, 0], ymax = Chosen[i, 3], color = color_chosen, linewidth = 4)                
        if Chosen[i, 3] < Chosen[i, 0]:
            color_chosen = 'red'
            plt.vlines(x = i, ymin = Chosen[i, 3], ymax = Chosen[i, 0], color = color_chosen, linewidth = 4)  
            
        if Chosen[i, 3] == Chosen[i, 0]:
            color_chosen = 'black'
            plt.vlines(x = i, ymin = Chosen[i, 3], ymax = Chosen[i, 0], color = color_chosen, linewidth = 4)  
          
    plt.grid()
    plt.title(name)
# Using the function
ohlc_plot(my_ohlc_data, 50, '')