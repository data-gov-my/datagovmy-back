import os
from typing import List

import environ
import requests

env = environ.Env()
environ.Env.read_env()

"""
Sends a telegram message
"""


def format_header(text: str):
    text = text.strip()
    # text = f" {text} "
    # text = text.center(50, char)
    text = f"<b>{text}</b>\n"
    return text


def format_files_with_status_emoji(files: List[str], emoji: str):
    return "\n\n".join(f"{emoji}: {file}" for file in files)


def send_telegram(message: str):
    location = format_header(os.getenv("ENV_LOCATION")) + "\n"
    message = location + message

    params = {
        "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
        "text": message,
        "parse_mode": "HTML",
    }
    tf_url = f'https://api.telegram.org/bot{os.getenv("TELEGRAM_TOKEN")}/sendMessage'
    r = requests.get(url=tf_url, data=params)


"""
Formats a telegram message
"""


def format_multi_line(arr: List[dict], header: str, pad_length=10):
    header = format_header(header)
    if arr:
        pad_length = len(max(arr[0], key=len))

    message = ""
    for obj in arr:
        cur_str = ""
        for k, v in obj.items():
            cur_str += f"<code>{k.ljust(pad_length)}</code>: {v}\n"
        cur_str += "\n\n"
        message += cur_str

    return f"{header}\n{message}"


def format_status_message(arr, header):
    str = header + "\n\n"

    for obj in arr:
        cur_str = obj["status"] + " : " + obj["variable"] + "\n"
        str += cur_str

    return str
