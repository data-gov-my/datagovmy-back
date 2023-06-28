import pandas as pd
import numpy as np
import operator
from django.apps import apps
from django.http import JsonResponse
from data_gov_my.explorers.General import General_Explorer


class NAME_POPULARITY(General_Explorer) :
    
    # General Data
    explorer_name = 'NAME_POPULARITY'

    # Data Update
    data_update = ''
    columns_exclude = []
    columns_rename = {'1920s': 'd_1920', '1930s': 'd_1930', 
                      '1940s': 'd_1940', '1950s': 'd_1950', 
                      '1960s': 'd_1960', '1970s': 'd_1970', 
                      '1980s': 'd_1980', '1990s': 'd_1990', 
                      '2000s': 'd_2000', '2010s': 'd_2010'}

    # Data Populate
    batch_size = 10000
    data_populate = {
        'NameDashboard_FirstName' : 'https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/name_popularity_first.parquet',
        'NameDashboard_LastName' : 'https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/name_popularity_last.parquet'
    }

    # API handling
    param_models = {'first' : 'NameDashboard_FirstName', 'last' : 'NameDashboard_LastName'}
    required_params = ['name', 'explorer', 'type']


    '''
    Constructor.
    '''
    def __init__(self) :
        General_Explorer.__init__(self)
    
    '''
    Handles the API requests,
    and returns the data accordingly.
    '''

    def handle_api(self, params) :
        # Validate Params Properly if exist
        if not self.is_params_exist(params) :
            return JsonResponse({"status": 400, "message": "Bad Request"}, status=400)

        s = params['name'][0].lower()
        type = params['type'][0].lower()
        dashboard = params['explorer'][0]

        if (type != 'first' and type != 'last') or dashboard != self.explorer_name:
            return JsonResponse({"status": 400, "message": "Bad Request"}, status=400)
        
        model_name = self.param_models[ type ]
        model_choice = apps.get_model('data_gov_my', model_name)   

        compare = 'compare_name' in params and self.str2bool(params['compare_name'][0])
        
        s = list(set(s.split(","))) if compare else [s]
        res = model_choice.objects.all().filter(name__in=s).values()

        fin = [] # Default is as a list

        if len(res) > 0 : 
            for i in res : 
                temp = {}
                temp['name'] = i['name']
                temp['total'] = i['total']
                i.pop('name')
                i.pop('total')
                
                if compare :
                    temp['max'] = [key.replace('d_', '') for m in [max(i.values())] for key,val in i.items() if val == m][-1]
                    temp['min'] = [key.replace('d_', '') for m in [min(i.values())] for key,val in i.items() if val == m][-1] 
                    
                    fin.append(temp)
                    s.remove(temp['name'])
                else : 
                    temp['decade'] = [d.replace("d_", "") for d in list(i.keys())]
                    temp['count'] = list(i.values())
                    fin = temp # Convert back into Dictionary
                    break
            
        if len(s) > 0 and compare:
            for name in s :
                fin.append({"name" : name, "total" : 0, "max" : "-", "min" : "-"})

        last_update = self.get_last_update(model_name=model_name)
        res = {"last_update" : last_update, "data" : fin}
        return JsonResponse(res, safe=False, status=200)
