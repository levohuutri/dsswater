import pandas as pd
from pandas.api.types import is_numeric_dtype
pd.options.mode.copy_on_write = True
import streamlit as st
import locale
from locale import atof
import numpy as np
    
def dss2_level(wsci_value):
        level = ''
        if wsci_value is not None:
            if wsci_value > 1.0:
                level =  "Rất cao"
            elif wsci_value  >= 0.75 and wsci_value  <= 1.0:
                level =  "Cao"
            elif wsci_value >=0.50 and wsci_value  < 0.75:
                level =  "Khá"
            elif wsci_value >= 0.25 and wsci_value  <0.5:
                level =  "Thấp"
            elif wsci_value >= 0.0 and wsci_value <0.25:
                level =  "Rất thấp"
        return level    
    
def dss2_color(wsci_value):
    color = ''
    if wsci_value is not None:
        if wsci_value > 1.0:
            color = 'RGB(51,51,255)'
        elif wsci_value  >= 0.75 and wsci_value  <= 1.0:
            color = 'RGB(0,228,0)'
        elif wsci_value >=0.50 and wsci_value  < 0.75:
            color =   'RGB(255,255,0)'
        elif wsci_value >= 0.25 and wsci_value  <0.5:
            color =  'RGB(255,126,0)'
        elif wsci_value >= 0.0 and wsci_value <0.25:
            color =  'RGB(255,0,0)'
    return color    
    

# Calculate dss2
def dss2_final (input, status_callback):
    wsci = pd.read_csv(input,encoding = "UTF-8") # depend on exisitng dss2.csv file 
    # locale.setlocale(locale.LC_NUMERIC, '') 

    wsci[['QSH', 'QCN', 'QTM-DV','Qi', 'KS','QSP', 'QT','fT','QAN','fAN','QAQ','fAQ','QRE2',\
         'QNN','QRE3','Qs','fs','Ws','Qg','fg','Wg','Qr','fr','Wr','Wru','WSCI1','WSCI2','WSC3']].apply(pd.to_numeric)
    
    # for i in ['QSH', 'QCN', 'QTM-DV','Qi', 'KS','QSP', 'QT','fT','QAN','fAN','QAQ','fAQ','QRE2',\
    #      'QNN','QRE3','Qs','fs','Ws','Qg','fg','Wg','Qr','fr','Wr','Wru','WSCI1','WSCI2','WSC3']:
    #     if (not is_numeric_dtype(wsci[i])):
    #         print('Kiểm tra dữ liệu đầu vào!')
    #         exit
            
    # wsci[['QSH', 'QCN', 'QTM-DV','Qi', 'KS']].applymap(atof)
    # wsci['QSH'] = wsci['QSH'].map(atof)

    # st.write(wsci.dtypes)
    # locale.setlocale(locale.LC_NUMERIC, '')
    # wsci.applymap(atof)
    i = 0
    steps =14 
    if 'Q_RE1' not in wsci.columns:
        wsci.insert(10, 'Q_RE1', None)  
    wsci['Q_RE1']  = round(sum([wsci['QSH'], wsci['QCN'],wsci['QTM-DV']]),0)
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate Q_RE1'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 

    if 'Q_SP' not in wsci.columns:
        wsci.insert(11, 'Q_SP', None)  
    wsci['Q_SP']  = wsci['Qi']*wsci['KS']

    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate Q_SP'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 

    if 'Q_RE2' not in wsci.columns:
        wsci.insert(12, 'Q_RE2', None)  
    wsci['Q_RE2']  = round((wsci['Q_RE1']+ (wsci['QT']*wsci['fT']) + (wsci['QAN']*wsci['fAN'])+(wsci['QAQ']*wsci['fAQ'])),0)
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate Q_RE2'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    
    if 'Q_NN' not in wsci.columns:
        wsci.insert(13, 'Q_NN', None)  
    wsci['Q_NN']  = wsci['QT']+ wsci['QAN']+wsci['QAQ']
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate Q_NN'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    

    if 'Q_RE3' not in wsci.columns:
        wsci.insert(14, 'Q_RE3', None)  
    wsci['Q_RE3']  = wsci['Q_NN'] - wsci['Q_RE2']
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate Q_RE3'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    

    if 'Q_s' not in wsci.columns:
        wsci.insert(15, 'Q_s', None)  
    wsci['Q_s']  = (wsci['QT']+ wsci['QAN']+wsci['QAQ'])/0.001
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate Q_s'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    

    if 'W_s' not in wsci.columns:
        wsci.insert(16, 'W_s', None)  
    wsci['W_s']  = wsci['Q_s']*wsci['fs']
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate W_s'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    
    if 'Q_g' not in wsci.columns:
        wsci.insert(17, 'Q_g', None)  
    wsci['Q_g']  = wsci['Q_s']*0.0008
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate Q_g'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 


    if 'W_g' not in wsci.columns:
        wsci.insert(18, 'W_g', None)  
    wsci['W_g']  = wsci['Q_g']* wsci['fg']
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate W_g'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    

    if 'W_r' not in wsci.columns:
        wsci.insert(19, 'W_r', None)  
    wsci['W_r']  = wsci['Qr']* wsci['fr']
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate W_r'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    

    if 'W_ru' not in wsci.columns:
        wsci.insert(20, 'W_ru', None)  
    wsci['W_ru']  = wsci['W_s']+wsci['W_g']+wsci['W_r']
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate W_ru'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    

    
    if 'W_SCI1' not in wsci.columns:
        wsci.insert(21, 'W_SCI1', None)  
    wsci['W_SCI1']  = wsci['Q_SP']/wsci['Q_RE1']
    if 'W_SCI1_Level' not in wsci.columns:
        wsci.insert(22, 'W_SCI1_Level', '') 
    wsci['W_SCI1_Level']  = wsci['W_SCI1'].map(dss2_level)
    
    if 'W_SCI1_Color' not in wsci.columns:
        wsci.insert(23, 'W_SCI1_Color', '') 
    wsci['W_SCI1_Color']  = wsci['W_SCI1'].map(dss2_color)
    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate W_SCI1'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    
    if 'W_SCI2' not in wsci.columns:
        wsci.insert(24, 'W_SCI2', None)  
    wsci['W_SCI2']  = wsci['Q_SP']/wsci['Q_RE2']

    if 'W_SCI2_Level' not in wsci.columns:
        wsci.insert(25, 'W_SCI2_Level', '') 
    wsci['W_SCI2_Level']  = wsci['W_SCI2'].map(dss2_level)
    if 'W_SCI2_Color' not in wsci.columns:
        wsci.insert(26, 'W_SCI2_Color', '') 
    wsci['W_SCI2_Color']  = wsci['W_SCI2'].map(dss2_color)

    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate W_SCI2'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    
    if 'W_SCI3' not in wsci.columns:
        wsci.insert(27, 'W_SCI3', None)  
    wsci['W_SCI3']  = wsci['W_ru']/wsci['Q_RE3']

    if 'W_SCI3_Level' not in wsci.columns:
        wsci.insert(28, 'W_SCI3_Level', '') 
    wsci['W_SCI3_Level']  = wsci['W_SCI3'].map(dss2_level)
    if 'W_SCI3_Color' not in wsci.columns:
        wsci.insert(29, 'W_SCI3_Color', '') 
    wsci['W_SCI3_Color']  = wsci['W_SCI3'].map(dss2_color)

    i+=1
    percent = int((i/steps)*100)
    label = str(i)+ '/'+ str(steps)+ '. Calculate W_SCI3'    
    if status_callback:
        status_callback(percent,label)
    else:
        print(label) 
    
    
    # st.write(wsci.dtypes)
    return wsci