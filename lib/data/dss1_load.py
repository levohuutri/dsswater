from .dss_load import dss_load

def dss1_load(start_at, end_at):
    return dss_load(mn_name='DSS-1', start_at=start_at, end_at=end_at)