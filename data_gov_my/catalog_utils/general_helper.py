import json

def read_json(file_name) :
    f = open(file_name, encoding="utf8")
    data = json.load(f)
    f.close()
    return data