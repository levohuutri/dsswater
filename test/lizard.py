# # try:
# #     from osgeo import gdal
# # except ImportError:
# #     import gdal
# #     #from osgeo import ogr
# import os
# #os.getcwd()
# import pandas as pd
# import geopandas as gpd
# import requests
# import streamlit as st
# import folium as f
# from streamlit_folium import st_folium

# # import geemap

# geojson = os.path.join(os.path.dirname(__file__), 'dss1.geojson')
# csv = os.path.join(os.path.dirname(__file__), 'dss1.csv')
# # 3lAj61vj.H5YQXu95lhkyN599SobVaapydeOiU21j
# USERNAME = "__key__"
# PASSWORD = "3lAj61vj.H5YQXu95lhkyN599SobVaapydeOiU21j"
# # PASSWORD ="EMkFOzzE.l2Cs8pneerhQHfFZyjc1UBUX6cZLu0it"
# HEADERS = {
#     "username": USERNAME,
#     "password": PASSWORD,
#     "Content-Type": "application/json"
#     }
# # mn_name= "DSS1-1.0.1"
# mn_name="DSS-2"

# url = "https://demo.lizard.net/api/v4/monitoringnetworks/?name__icontains=" + mn_name
# r = requests.get(url, headers = HEADERS)
# # print (r.json())

# mn = r.json()["results"]
# # print(mn)

# url2 = "https://demo.lizard.net/api/v4/monitoringnetworks/"+mn[0]['uuid']+"/timeseries/"
# # print (url2)
# # querydata = requests.get(url2,headers=HEADERS,params= {'page_size':'10000'}).json()['results']
# # timeseries_list = pd.DataFrame(querydata)
# # st.write(timeseries_list[0:4])
# # print(querydata)

# # print(timeseries_list[0:4])# print only the first 5 rows of the dataframe
# # url3 = timeseries_list['url'][0]
# # r = requests.get(url3, headers = HEADERS)
# # # print(r.json()["code"])
# # # print(r.json()["location"])

# # url4 = timeseries_list['url'][0]+"events/"
# # # print(url)
# # time_series_events = pd.DataFrame(requests.get(url=url4,headers=HEADERS,params= {'page_size':'10000'}).json()['results'])

# # # print(time_series_events[0:5])
# # st.write(time_series_events[0:5])
# # st.latex(" \int f^{-1}(x-x_a)\,dx")

# # center on Liberty Bell, add marker
# # m = f.Map(location=[ 9.3, 105.68], zoom_start=8)
# # folium.Marker(
# #     [9.3,105.68], popup="Hellow World", tooltip="Hellow World"
# # ).add_to(m)
# # folium.GeoJson(geojson,
# #                tooltip=folium.GeoJsonTooltip(fields=['Matram'])
# #               ).add_to(m)
# # st_data = st_folium(m, width=725)


# #Loop through each row in the dataframe


# df = gpd.read_file(geojson)
# df['lon'] = df.geometry.x  # extract longitude from geometry
# df['lat'] = df.geometry.y  # extract latitude from geometry
# df = df[['lon','lat','Matram']]     # only keep longitude and latitude
# st.write(df.head())        # show on table for testing only
# st.map(df)  
# # m = f.Map(location=[df.lat.mean(), df.lon.mean()], 
# #                  zoom_start=8, control_scale=True)

# # for i,row in df.iterrows():
# #     #Setup the content of the popup
# #     iframe = f.IFrame('Mã trạm:' + str(row["Matram"]))
    
# #     #Initialise the popup using the iframe
# #     popup = f.Popup(iframe, min_width=200, max_width=200)
    
# #     #Add each row to the map
# #     f.Marker(location=[row['lon'],row['lat']],
# #                   popup = popup, c=row['Matram']).add_to(m)

# # f.TileLayer('stamenwatercolor').add_to(m)
# # f.LayerControl().add_to(m)
# # st_folium(m, width=725)


# # call to render Folium map in Streamlit

# # st.write(r.json())

# # df= pd.read_csv(csv, skiprows=[1], on_bad_lines='skip')
# # # print (df)
# # gdf = gpd.GeoDataFrame(
# #     df, geometry=gpd.points_from_xy(df.E, df.N)
# # )
# # print (gdf)
# # gdf.to_file(os.path.join(os.path.dirname(__file__), 'dss1.geojson'), driver='GeoJSON')  

