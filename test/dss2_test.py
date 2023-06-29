import pandas as pd
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))
from datetime import timedelta

from dss2 import dss2_final

dss1_file = os.path.join(os.path.dirname(__file__), '../data/dss2.csv')
dss1_df = pd.read_csv(dss1_file)
# fromdate = dss1_df['Date'].min()
# todate =  dss1_df['Date'].max()
print (dss2_final(dss1_file, None))