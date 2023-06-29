import pandas as pd
import requests
import asyncio

global_is_dss_loading = False
async def dss_load(mn_name, start_at, end_at, ui):
    global global_is_dss_loading
    if (global_is_dss_loading and False): return pd.DataFrame()
    global_is_dss_loading = True

    username = '__key__'
    password = 'dHrkmkoz.aeLBK4c0wW0z2OIQWmqWF9QajHARwDQQ' # change password to your API key
    json_headers = {
                "username": username,
                "password": password,
                "Content-Type": "application/json",
    }
    filter_locations = {
            "M3-R": {"lat": "10.89","lng": "106.69", "z": 0.27},
            "BD18": {"lat": "10.422778","lng": "106.4375", "z": 1},
            "N2": {"lat": "11.317778","lng": "106.19", "z": 0.17},
            "M1-L": {"lat": "10.99","lng": "106.617222", "z": 0.51},
            "BD06": {"lat": "10.378889","lng": "106.489167", "z": 1.03},
            "M2-R": {"lat": "10.096667","lng": "106.65", "z": 0.17},
            "CLCB9": {"lat": "10.43830833","lng": "105.0572111", "z": -0.07},
            "OX01": {"lat": "10.080774","lng": "105.639204", "z": 0.97},
            "QL1": {"lat": "9.300097108525588","lng": "105.6767805694728", "z": 0.66},
            "MT01": {"lat": "10.02","lng": "105.99", "z": 1.36},
            "DĐ1": {"lat": "10.07","lng": "105.1751", "z": 0.5},
            "TGLX6": {"lat": "10.525283","lng": "104.644658", "z": 0.33}
    }
    baseUrl = "https://demo.lizard.net/api/v4"
    # start_at = '2020-09-30T00:00:00Z'
    # end_at = '2023-06-27T00:00:00Z'
    # mn_name = "DSS-1"
    # Request Timeseries
    # Call the Lizard API V4 monitoring network endpoint:
    url = baseUrl + "/monitoringnetworks/?name__icontains="+mn_name

    # Retrieve the 'results' attribute using a JSON interpreter
    mn = requests.get(url,headers=json_headers).json()['results']
    show_mes(ui, 'The UUID of this monitoring network is ' + mn[0]['uuid'])

    arr_df = []
    async def requestTimeSeries(location_code):
        show_mes(ui, 'Get timeseries of ' + location_code)
        url = baseUrl + "/monitoringnetworks/"+mn[0]['uuid']+"/timeseries/?location__name=" + location_code
        querydata = requests.get(url,headers=json_headers,params= {'page_size':'1000000'}).json()['results']
        q_df = pd.DataFrame(querydata)
        arr_df.append(q_df)
    # Call the monitoringnetworks endpoint with its uuid to retrieve the timeseries belowing to this monitoring network
    coroutines = []
    for location_code in filter_locations:
        coroutines.append(requestTimeSeries(location_code=location_code))

    await asyncio.gather(*coroutines)

    timeseries_list = pd.concat(arr_df)
    timeseries_list.reset_index(drop=True)
    timeseries_list.shape[0]
    # Query time series
    # call the timeseries endpoint for number X timeseries of list
    time_series_events = {}
    params= {'start':start_at, 'end':end_at,'page_size':'10000000'}
    num = timeseries_list.shape[0]
    #len = 10
    urls = timeseries_list['url'].to_list()
    codes = timeseries_list['code'].to_list()
    locations = timeseries_list['location'].to_list()
    async def requestTimeSeriesEvents(X):
        url = urls[X]+"events/"
        code = codes[X]
        location = locations[X]
        l_code = location['code']
        show_mes(ui, f"Get events: {X}/{num} " + l_code + "-" + code)
        querydata = requests.get(url=url,headers=json_headers,params=params).json()['results']
        time_series_event = pd.DataFrame(querydata)
        time_series_event['Kyhieu'] = l_code
        time_series_event['Matram'] = l_code
        time_series_event['latitude'] = filter_locations[l_code]["lat"]
        time_series_event['longitude'] = filter_locations[l_code]["lng"]
        time_series_event['Z.Elev'] = filter_locations[l_code]["z"]
        try:
            if not code in time_series_events:
                time_series_events[code] = time_series_event
            else:
                if isinstance(time_series_event, pd.DataFrame):
                    time_series_events[code] = pd.concat([time_series_events[code], time_series_event])
        except Exception as e:
        # Handling the exception
            show_mes(ui, f"An exception occurred: {e}")
    coroutines = []
    for X in range(0, num):
        coroutines.append(requestTimeSeriesEvents(X))

    await asyncio.gather(*coroutines)
    
    from datetime import datetime

    # Function to convert time value
    def convert_time(time_str):
        # Convert string to datetime object
        datetime_obj = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%SZ")
        # Convert datetime object to desired format
        formatted_time = datetime_obj.strftime("%d/%m/%Y")
        return formatted_time

    arr_filter = {}
    for key in time_series_events:
        columns = time_series_events[key].columns.to_list()
        if 'time' in columns and 'value' in columns:
            # try:
            time_series_events[key]['Date'] = time_series_events[key]['time'].apply(convert_time)
            arr_filter[key] = time_series_events[key]
            show_mes(ui, f"df {key} converted time to d/m/Y")

    merged_df = pd.concat([
    arr_filter[key].set_index(['Matram', 'Kyhieu', 'Date', 'latitude', 'longitude', 'Z.Elev'])['value'].rename(key) 
    for key in arr_filter if 'value' in arr_filter[key].columns.to_list()
    ], axis=1)
    # Creating an empty row with NaN values
    empty_row = pd.DataFrame([[None] * merged_df.columns.__len__()], columns=merged_df.columns)
    # Inserting the empty row at the first position
    merged_df = pd.concat([merged_df])
    merged_df = merged_df.fillna(0)
    merged_df = merged_df.rename(columns={
            "E.coli.": "Ecoli",
            "Heptachlorepoxide.": "Heptachlorepoxide",
            "N-NH4": "N_NH4",
            "N-NO2": "N_NO2",
            "N-NO3": "N_NO3",
            "P-PO4": "P_PO4"
    })
    merged_df['T'] = 26.5
    merged_df['BOD5'] = 10
    merged_df['DO'] = 4.5
    merged_df['Salinity'] = 0.5
    filename = mn_name + "data.csv"
    merged_df.to_csv(filename)
    is_dss_loading = False
    return merged_df

def show_mes(ui, mes):
    if ui:
        ui.update_mes(mes)
    print(mes)

def dss_load_sync(mn_name, start_at, end_at, ui):
    value = asyncio.run(dss_load(mn_name=mn_name, start_at=start_at, end_at=end_at, ui=ui))
    return value