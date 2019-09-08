# -*- coding: utf-8 -*-
"""
Created on Tue Nov 20 14:53:18 2018

@author: NishantParmar
"""

import pandas as pd
import os

BASE_FOLDER_PATH = 'C:\\Users\\NishantParmar\\Documents\\Python Scripts\\AirQualityAnalysis\\Output'

def get_folder_contents(startpath):
    folder_dict = dict()
    for root, dirs, files in os.walk(startpath):
        if os.path.abspath(root) != BASE_FOLDER_PATH:
            folder_dict[os.path.basename(root)] = list()
            for f in files:
                folder_dict[os.path.basename(root)].append(f)
    return folder_dict

def process_data(repository):
    for date, file_list in repository.items():
        if file_list:
            file_path = BASE_FOLDER_PATH + '\\{}\\{}'.format(date, file_list[0])
            file_date = int(file_path.split('\\')[-2].split('-')[-1][:2])
            if file_date > 20:
                data_source = pd.read_csv(file_path)
                
                # Cleaning the DatFrame columns:
                data_source.rename(lambda x: x.replace('   ', '_').replace(' ', ''), axis='columns', inplace=True)
                print(list(data_source.columns))
                grouped_df = pd.DataFrame({'pollution_data':data_source.groupby('city').apply(lambda x: ','.join(x.pollutant_min))})
                print(grouped_df)
#                grouped_df = pd.DataFrame({'pollution_data': data_source.groupby('city').apply(lambda x, y, z: d[k].append(v) for k, v in zip(x.pollutant_id, [y.pollutant_min, z.pollutant_max]))})
folder_contents = get_folder_contents(BASE_FOLDER_PATH)
process_data(folder_contents)