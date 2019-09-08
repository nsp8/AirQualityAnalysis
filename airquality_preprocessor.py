# -*- coding: utf-8 -*-
"""
Created on Fri Oct 19 11:54:28 2018

@author: NishantParmar
"""
import constants
import csv
import errno
import os
import pandas as pd
import re
import requests

api_key = constants.api_key
resource = constants.resource_id
url = 'https://api.data.gov.in/resource/{}'
url += '?api-key={}&format={}&limit={}'.format(resource, api_key, 'csv', 1000)
response = requests.get(url)
data_list = None
empty_response = True
if response.text:
    empty_response = False
    data_list = response.text.split('\n')
else:
    print('Response message: empty\n[Status Code:{}]'
          .format(response.status_code))

output_dir = constants.output_base_path
output_file_path = os.path.join(output_dir, 'Output', 'AirQualityData.csv')

# Redundant data is present (always) here:
DUPLICACY_INDEX = 4


def remove_empty(data_collection):
    """
    Removes the redundant empty values in the data-set, if any.
    """
    new_list = list()
    for item in data_collection:
        if len(item.split(',')) >= 11:
            new_list.append(item)
    return new_list


def clean_data(data_list):
    """
    Cleans and formats individual values in the data-set.
    """    
    new_list = list()
    for data_ in data_list:
        anomalous = re.findall('\".*\"', data_)
        if anomalous:
            anomalous_part = anomalous[0]
            reformed_str = anomalous_part.replace('"', '').replace(', ', ': ')
            data_ = data_.replace(anomalous_part, reformed_str)
        data_arr = data_.split(',')
        data_split = [item for item in map(lambda x: x.replace('_', ' '), data_arr)]
        if len(data_split) > max_column_length:
            data_split = data_split[:DUPLICACY_INDEX] + data_split[DUPLICACY_INDEX+1:]
    
        new_list.append(data_split)
    return new_list


def data_writer(data, dest_file):
    """
    Stores the processed values in a CSV file using CSV writer.
    """    
    create_subdirectory(dest_file)
    with open(dest_file, 'a', newline='\n') as f:
        writer = csv.writer(f, delimiter=' ', escapechar=' ',
                            quoting=csv.QUOTE_NONE)
        for row in data:
            if len(row.split(',')) > len(data[0].split(',')):
                row_reduced = row.split(',')
                del row_reduced[DUPLICACY_INDEX]
                row = ','.join(row_reduced)
            writer.writerow(row)


def create_subdirectory(output_file_path):
    """
    Creates the new date-wise directory (if it doesn't exist) to store data in 
    separate folders.
    """      
    if not os.path.exists(os.path.dirname(output_file_path)):
        try:
            os.makedirs(os.path.dirname(output_file_path))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def combine_pollution_data(data):
    pollution_dict = dict()
    pollution_dict['pollutant'] = data['pollutant id']
    pollution_dict['pollutant min'] = data['pollutant min']
    pollution_dict['pollutant max'] = data['pollutant max']
    pollution_dict['pollutant avg'] = data['pollutant avg']
    return pd.DataFrame(pollution_dict)


def extract_pollution_data(data_combined):
    test_obj = dict()
    test_items = list()
    data_df = pd.DataFrame(data_combined[1:], columns=data_combined[0])
    test_items.append(('Pollutant_info', data_df.groupby('city').apply(combine_pollution_data)))
    test_obj.update(test_items)
    return test_obj



if not empty_response:
    # Removing empty values 
    data_list = remove_empty(data_list)
    
    # Marking the maximum columns that the data should have
    try:
        max_column_length = len(data_list[0].split(','))
    except IndexError:
        print('data_list = {}'.format(data_list))    
    data_list_improved = clean_data(data_list)

    # Saving the cleansed data in a CSV file using Pandas
#    pandas_writer(data_list_improved, output_file_path)
    print(extract_pollution_data(data_list_improved))

# Using the CSV package to write the data in a CSV file
#data_writer(data_list_improved, output_file_path)

# Creating a JSON object of the response from data.gov.in
#data_df = pd.DataFrame(json.loads(response.text))

"""
TODO: 
    pandas_writer: 
        > check for existing data: append the updated data only
        > Don't write the header in case of appending data
        > Update the ID as well.
"""