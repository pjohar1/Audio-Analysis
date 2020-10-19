# -*- coding: utf-8 -*-
"""
Created on Tue Oct 13 18:44:56 2020

@author: punar
"""

#import os
#import numpy as np
import pandas as pd 

def get_operator(tup):
    name = tup['speakername']
    date = tup['new_date']
    if 'OPERATOR' in name:
        return name 
    else:
        return name
    
def get_role(tup):
    
    if tup['CEO'] == 1:
        return 'CEO'
    elif tup['CFO'] == 1:
        return 'CFO'
    elif tup['COO'] == 1:
        return 'COO'
    elif tup['Analyst'] == 1:
        return 'Analyst'
    elif tup['Operator'] == 1:
        return 'Operator'
    else:
        return 'Blank'
    
    
def analyst_counter(info):
    #print(info.split(',')[0])
    if 'ANALYST' in info.split(',')[0]:
        return 1
    else:
        return 0
    
def operator_counter(name):
    if 'OPERATOR' in name:
        return 1
    else:
        return 0
    
    
    
def process_file(name):
    print(name)
    
    dir_timestamp = '/data/project/audio_tone/all_files/Timestamps Tickers/'
    dir_max_time = '/data/project/audio_tone/all_files/Max Speaker Time Ticker/'
    
    ticker_local = name.split('_')[0]
    #print(ticker_local)
    data = pd.DataFrame(columns = ['ticker', 'new_date', 'speakername', 'rawspeakerinfo', 
                                       'additionalspeakerinfo', 'CEO', 'CFO','COO', 'start_time', 'end_time'])
    df = pd.read_csv(dir_timestamp + ticker_local + '/'+ name)
    
    #df['speakername'] = df[['speakername','new_date']].apply(lambda x: get_operator(x), axis = 1)
    
    df['additionalspeakerinfo'].fillna('Blank', inplace = True)
    try:
        data = pd.concat([data,
                        df[['ticker', 'new_date', 'speakername', 'rawspeakerinfo', 'additionalspeakerinfo', 'CEO', 'CFO','COO', 'start_time', 'end_time']]], ignore_index = True)
    except:
        print('Sorry')
            
    data.columns = ['Ticker', 'Date', 'Name', 'RawSpeakerInfo', 'AdditionalSpeakerInfo', 'CEO', 'CFO','COO', 'Start_sec', 'End_sec']
        
    data = data[data['Start_sec'] != 'Not Found']
    data = data[data['End_sec'] != 'Not Found']

    # Calculate the length of time segment

    data['End_sec'] = data['End_sec'].astype(float) 
    data['Start_sec'] = data['Start_sec'].astype(float)
    data['diff'] = data['End_sec'] - data['Start_sec']  

    data['Operator'] = data['Name'].apply(lambda x: operator_counter(x))
    data['Analyst'] = data['AdditionalSpeakerInfo'].apply(lambda x: analyst_counter(x))

    try:
        data['Role'] = data[['CEO', 'CFO', 'COO', 'Analyst', 'Operator']].apply(lambda x: get_role(x), axis = 1)
    except:
        print('Another sorry')

    group = data.groupby('Name').max('Diff')

    data = data.merge(group, how='inner', left_on=['Name', 'diff'], right_on=['Name', 'diff'])

    try:
        del data['Start_sec_y']
        del data['End_sec_y']
        del data['Analyst_y']
        del data['Operator_y']

    except:
        print('Another one')

    data.rename(columns = {'Start_sec_x': 'Start_sec', 'End_sec_x': 'End_sec', 
                           'Operator_x': 'Operator', 'Analyst_x': 'Analyst'}, inplace=True)
    
    date = name.split('_')[1]
    #print(date)
    
    data.to_csv(dir_max_time + ticker_local +'/'+ ticker_local +'_'+ date+'_' +'max_time.csv', index=False)
    