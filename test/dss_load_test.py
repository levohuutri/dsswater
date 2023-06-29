import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))
from data.dss_load import dss_load_sync
import asyncio


dss_load_sync(
        mn_name="DSS-1", 
        start_at='2016-02-29T00:00:00Z', 
        end_at='2023-06-28T00:00:00Z',
        ui=None)
