import streamlit as st
# import os
import pandas as pd
import geopandas as gpd
import numpy as np
import datetime
# import json
# import requests
# from streamlit_folium import st_folium, folium_static
# from pandas.api.types import is_numeric_dtype
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'lib'))
from dss1 import dss1_final, _dss1_final
from dss2 import dss2_final
from data import dss_load
import leafmap.foliumap as leafmap

# from streamlit_extras.buy_me_a_coffee import button


from datetime import timedelta
st.set_page_config(
            page_title="DSS Calculation Module",
            page_icon=":aquarius:",
            layout="centered",
            initial_sidebar_state="auto",
            menu_items={
                'Report a bug': "https://github.com/thangqd/dss",
                'About': "https://thangqd.github.io"
            }
)
is_loading_wh = False
class dss():    
    def __init__(self):
        st.header("DSS Calculation Module")
        st.subheader("©2023 by watertech.vn") 
        self.is_loading_wh = False  

        
    def gui(self):       
        self.datasources = st.selectbox('Choose DataSource', ('UploadFile', 'Lizard'))
        if (self.datasources):
            if (self.datasources == "UploadFile"):
                self.uploaded_file = st.file_uploader("Choose a CSV file")
                # self.uploaded_file = os.path.join(os.path.dirname(__file__), '../data/dss1.csv')         

                if (self.uploaded_file):
                    # st.write(uploaded_file)     
                    self.dss_calc = st.selectbox('Choose DSS to calculate',('DSS1','DSS2','DSS3','DSS4','DSS6'))
                    self.fromdate  = st.date_input("From date", pd.to_datetime('today')- timedelta(days=1000))
                    self.todate = st.date_input("To date", pd.to_datetime('today'))         
                            
                    # st.button('Load data', on_click=self.loadata(self.uploaded_file,self.fromdate,self.todate))
                    self.status_lable ="Calculation progress"
                    status_bar = st.progress(0, text=self.status_lable)
                    self.status_bar= status_bar
                    self.dss_result = self.calculate_dss(self.uploaded_file,self.fromdate,self.todate, self.dss_status_callback)
                    st.button('Calculate DSS', on_click=self.dss_result)    
                # st.button('View map', on_click=self.viewmap(self.uploaded_file, self.dss_status_callback))
            elif (self.datasources == "Lizard"):    
                self.fromdate  = st.date_input("From date", pd.to_datetime('today')- timedelta(days=3000))
                self.todate = st.date_input("To date", pd.to_datetime('today'))         
                self.dss_calc = st.selectbox('DSS',('DSS1','DSS2','DSS3','DSS4','DSS6'))
                self.status_lable = "Downloading data"
                self.status_bar = st.progress(0, text=self.status_lable)
                
                if (not self.is_loading_wh):
                    self.is_loading_wh = True
                    df = self.wh_lizard_load(self)
                    self.status_lable ="Calculation progress"
                    status_bar = st.progress(0, text=self.status_lable)
                    self.status_bar= status_bar
                    self.dss_result = self.calculate_dss("/Users/trilvh/Desktop/DSS/CODE/dss/test/DSS-1data.csv",self.fromdate,self.todate, self.dss_status_callback)
                    st.button('Calculate DSS', on_click=self.dss_result)    
                    self.is_loading_wh = False
      
        st.divider()
        st.caption("Code by Thang Quach")
        # button(username="quachdongthang", floating=False, width=221)

    def wh_lizard_load(self, ui):  
        def convert_time(date):
            return date.strftime("%Y-%m-%dT%H:%M:%SZ")
        dss = 'DSS-1'
        df = dss_load.dss_load_sync(dss, convert_time(self.fromdate), convert_time(self.todate), ui)
        return df
    
    def update_mes(self, mes):
        self.status_bar.progress(0, text=mes)


    
    def loadata(self, input, fd, td):    
        # df_filter = df.loc[fromdate:todate]
        df = pd.read_csv(input,skiprows=[1])
            
        df["Date"] =  pd.to_datetime(df["Date"],dayfirst=True) # convert Date field to     
        df_filter = df.loc[(df['Date'] >= fd) & (df['Date']<= td)]
        st.dataframe(df_filter)
        # st.write(df_filter.dtypes)
        st.write(df_filter.describe()) 
        st.map(df_filter)
        result = df_filter.select_dtypes(include='number')
        st.line_chart(result)
        return df_filter

    def calculate_dss_lizard(self, df, fd, td, dss_status_callback = None):
        if self.dss_calc == "DSS1":
            dss1 = _dss1_final(df,fd,td,self.dss_status_callback)
            dss1["Date"] = pd.to_datetime(dss1["Date"]).dt.date
            try:
                st.dataframe(dss1.style.applymap(self.color,subset=['WQI_Color']))          
            except: st.write(dss1)
            self.download_csv(dss1,self.dss_status_callback)
            self.download_geojson(dss1,self.dss_status_callback)
            self.viewmap_dss1(dss1,dss_status_callback = None)
    
    def calculate_dss(self, input, fd, td, dss_status_callback = None):
        if self.dss_calc == "DSS1":
            dss1 = dss1_final(input,fd,td,self.dss_status_callback)
            print(dss1.columns.to_list())
            dss1["Date"] = pd.to_datetime(dss1["Date"]).dt.date
            try:
                st.dataframe(dss1.style.applymap(self.color,subset=['WQI_Color']))          
            except: st.write(dss1)
            self.download_csv(dss1,self.dss_status_callback)
            self.download_geojson(dss1,self.dss_status_callback)
            self.viewmap_dss1(dss1,dss_status_callback = None)
        elif self.dss_calc == "DSS2":
            dss2 = dss2_final(input,self.dss_status_callback)
            try:
                st.dataframe(dss2.style.applymap(self.color,subset=['W_SCI1_Color','W_SCI2_Color','W_SCI3_Color']))          
            except: st.write(dss2)
            self.download_csv(dss2,self.dss_status_callback)
            self.download_geojson(dss2,self.dss_status_callback)
            self.viewmap_dss2(dss2,dss_status_callback = None)          
            
    
    def color(self,val):
        return f'background-color: {val}'    
    
    def viewmap_dss1(self, df,dss_status_callback = None):        
        # st.map(df)
        if not df.empty:
            st.write('Select a date range to view map')
            fd  = st.date_input("From date", pd.to_datetime(min(df['Date'])))
            td = st.date_input("To date", pd.to_datetime(max(df['Date']))  ) 
            # selected_date = pd.to_datetime(selected_date)
            df["Date"] = pd.to_datetime(df["Date"]).dt.date  
            df_filter = df.loc[(df['Date'] >= fd) & (df['Date']<= td)]
            # st.write(selected_date)
            if not df_filter.empty:
                st.write(df_filter)
                m = leafmap.Map(center=[10.045180, 105.78841], zoom=8, tiles = 'Stamen Toner')
                m.add_points_from_xy(
                    df_filter,
                    x="longitude",
                    y="latitude",
                    # color_column='WQI_Color',
                    # icon_names=['gear', 'map', 'leaf', 'globe'],
                    spin=True,
                    # add_legend=True,
                )

                m.to_streamlit(height=700)
    
    def viewmap_dss2(self, df,dss_status_callback = None):        
        # st.map(df)
          if not df.empty:
            m = leafmap.Map(center=[10.045180, 105.78841], zoom=8, tiles = 'Stamen Toner')
            m.add_points_from_xy(
                df,
                x="longitude",
                y="latitude",
                spin=True,
            )
            m.to_streamlit(height=700)
        
    def download_csv(self, df,dss_status_callback = None):  
        if not df.empty:
            if 'Date' in df.columns:  
                df['Date'] =  df["Date"].astype(str)
            # csv = df.to_csv(index=False).encode('UTF-8') 
            csv = df.to_csv(encoding ='utf-8')        
            click = st.download_button(
            label= "Download CSV " + self.dss_calc,
            data = csv,
            file_name= self.dss_calc + ".csv",
            mime = "text/csv",
            key='download-csv')        

           
    def download_geojson(self, df,dss_status_callback = None):
        if not df.empty:
            if 'Date' in df.columns:  
                df['Date'] =  df["Date"].astype(str)
            # st.write(df.dtypes) 
            gdf = gpd.GeoDataFrame(
                df, geometry=gpd.points_from_xy(df.longitude, df.latitude)
            )

            # st.write(gdf)
            geojson = gdf.to_json()  
            st.download_button(
                label="Download GeoJSON",
                file_name= self.dss_calc + ".geojson",
                mime="application/json",
                data=geojson
            ) 
       
             
    def dss_status_callback(self, percent_complete, lable):        
        self.status_bar.progress(percent_complete, text=lable)              

# if __name__ == '__main__':
ct = dss()
ct.gui()