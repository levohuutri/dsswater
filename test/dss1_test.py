import pandas as pd
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))
from datetime import timedelta

from dss1 import dss1_final

dss1_file = os.path.join(os.path.dirname(__file__), '../data/dss1.csv')
dss1_df = pd.read_csv(dss1_file,skiprows=[1])
dss1_df["Date"] =  pd.to_datetime(dss1_df["Date"]) # convert Date field to  
# fromdate = dss1_df['Date'].min()
# todate =  dss1_df['Date'].max()
fromdate = (pd.to_datetime('today')- timedelta(days=1000))
todate = (pd.to_datetime('today'))
print (dss1_final(dss1_file,fromdate,todate, None))