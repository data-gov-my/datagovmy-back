import os
from os import listdir
from os.path import isfile, join
import requests
import environ

env = environ.Env()
environ.Env.read_env()

"""
Sends a telegram message
"""


def send_telegram(message):
    location = "--- " + os.getenv("ENV_LOCATION") + " ---\n"
    message = location + message

    params = {"chat_id": os.getenv("TELEGRAM_CHAT_ID"), "text": message}
    tf_url = f'https://api.telegram.org/bot{os.getenv("TELEGRAM_TOKEN")}/sendMessage'
    r = requests.get(url=tf_url, data=params)


"""
Formats a telegram message
"""


def format_multi_line(arr, header):
    h_str = header + "\n\n"

    for obj in arr:
        cur_str = ""
        for k, v in obj.items():
            cur_str += str(k) + " : " + str(v) + "\n"
        cur_str += "\n\n"
        h_str += cur_str

    return h_str


def format_status_message(arr, header):
    str = header + "\n\n"

    for obj in arr:
        cur_str = obj["status"] + " : " + obj["variable"] + "\n"
        str += cur_str

    return str
