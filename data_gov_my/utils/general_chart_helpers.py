import json
import re

import pandas as pd

"""
State abbreviations to use for each chart / dashboard.
"""
STATE_ABBR = {
    "Johor": "jhr",
    "Kedah": "kdh",
    "Kelantan": "ktn",
    "Klang Valley": "kvy",
    "Melaka": "mlk",
    "Negeri Sembilan": "nsn",
    "Pahang": "phg",
    "Perak": "prk",
    "Perlis": "pls",
    "Pulau Pinang": "png",
    "Sabah": "sbh",
    "Sarawak": "swk",
    "Selangor": "sgr",
    "Terengganu": "trg",
    "W.P. Labuan": "lbn",
    "W.P. Putrajaya": "pjy",
    "W.P. Kuala Lumpur": "kul",
    "Malaysia": "mys",
}


def get_nested_keys(d, keys):
    """
    Gets all nested keys within a dictionary.
    """
    for k, v in d.items():
        if isinstance(v, dict):
            get_nested_keys(v, keys)
            keys["key_list"].append(k)
        else:
            if len(keys["value_obj"]) == 0:
                keys["value_obj"].append(dict({k: v}))
            else:
                keys["value_obj"][0][k] = v


def get_dict(d, keys):
    """
    Gets a value of a dictionary based on the nested keys.
    """
    for key in keys:
        d = d[key]
    return d


def set_dict(d, keys, value, operation):
    """
    Sets a dictionary based on the nested keys.
    """
    d = get_dict(d, keys[:-1])
    d[keys[-1]] = value


def convert_pq_to_csv(read_dir, export_dir, file_list):
    """
    Converts parquet files into csv.
    """
    for f in file_list:
        df = pd.read_parquet(read_dir + f + ".parquet")
        df.to_csv(export_dir + f + ".csv")


def print_page(filename, dict):
    """
    Prints a json page, from dict to json.
    """
    with open(filename, "w") as json_file:
        json.dump(dict, json_file)


def rename_labels(label, rname_dict):
    """
    Custom renaming for labels within a chart.
    """
    for k, v in rname_dict.items():
        label = re.sub(k, v, label)

    if label in STATE_ABBR.values():
        return label

    return label.replace("_", " ").title()
