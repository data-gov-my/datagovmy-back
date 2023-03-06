import pandas as pd
import json
import numpy as np
import copy
import ast
from mergedeep import merge
import re

'''
State abbreviations,
to use for each chart / dashboard
'''
STATE_ABBR = {'Johor': 'jhr',
              'Kedah': 'kdh',
              'Kelantan': 'ktn',
              'Klang Valley': 'kvy',
              'Melaka': 'mlk',
              'Negeri Sembilan': 'nsn',
              'Pahang': 'phg',
              'Perak': 'prk',
              'Perlis': 'pls',
              'Pulau Pinang': 'png',
              'Sabah': 'sbh',
              'Sarawak': 'swk',
              'Selangor': 'sgr',
              'Terengganu': 'trg',
              'W.P. Labuan': 'lbn',
              'W.P. Putrajaya': 'pjy',
              'W.P. Kuala Lumpur': 'kul',
              'Malaysia': 'mys'
            }

'''
Gets all nested keys,
within a dictionary
'''
def get_nested_keys(d, keys):
    for k, v in d.items():
        if isinstance(v, dict):
            get_nested_keys(v, keys)
            keys['key_list'].append(k)
        else :
            if len(keys['value_obj']) == 0 :
                keys['value_obj'].append(dict({k : v}))
            else :
                keys['value_obj'][0][k] = v

'''
Gets a value of a dictionary,
based on the nested keys
'''
def get_dict(d, keys):
    for key in keys:
        d = d[key]
    return d

'''
Sets a dictionary,
based on the nested keys
'''
def set_dict(d, keys, value, operation):
    d = get_dict(d, keys[:-1])
    d[keys[-1]] = value

'''
Converts parquet files into csv
'''
def convert_pq_to_csv(read_dir, export_dir, file_list) :
    for f in file_list :
        df = pd.read_parquet(read_dir + f + '.parquet')
        df.to_csv(export_dir + f + ".csv")

'''
Prints a json page,
from dict to json
'''
def print_page(filename, dict) : 
    with open(filename, 'w') as json_file:
        json.dump(dict, json_file)

'''
Custom renaming,
for labels within a chart
'''
def rename_labels(label, rname_dict) : 
    for k, v in rname_dict.items() :
        label = re.sub(k, v, label)

    if label in STATE_ABBR.values() : 
        return label

    return label.replace("_", " ").title()