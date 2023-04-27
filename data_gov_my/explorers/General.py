import pandas as pd
import numpy as np
from django.apps import apps
from django.http import JsonResponse
from data_gov_my.utils.general_chart_helpers import STATE_ABBR

class General_Explorer :
    # General Data
    explorer_name = ''

    # Data Update
    data_update = ''
    columns_rename = {}
    columns_exclude = []

    # Data Populate
    batch_size = 10000
    data_populate = {}

    # API handling
    param_models = {}
    required_params = []

    def __init__(self) :
        pass
    
    '''
    Handles the API requests,
    and returns the data accordingly.
    '''

    def handle_api(self, request_params) :
        print("API Handling")

    '''
    Clears the model specified.
    If none specified, it'll clear all models, 
    related to this class.
    '''

    def clear_db(self, model_name = '') :
        if model_name : 
            model_choice = apps.get_model('data_gov_my', model_name)
            model_choice.objects.all().delete()
        else :
            for k in list(self.data_populate.keys()) :
                model_choice = apps.get_model('data_gov_my', k)
                model_choice.objects.all().delete()

    '''
    Performs an update to the database.
    '''

    def update() :
        # Change once pipeline is changed too.
        print("Update database")

    '''
    Populates the table,
    assumes table is empty, 
    pre-population.
    '''

    def populate_db(self, table = '', rebuild=False) :
        if table : # If set, builds only the table requested
            self.bulk_insert(self.data_populate[table], table, rebuild, self.batch_size, self.columns_rename, self.columns_exclude)
        else : # Builds all tables set in the data_populate attribute
            for k, v in self.data_populate.items() :
                self.bulk_insert(v, k, rebuild, self.batch_size, self.columns_rename, self.columns_exclude)

    '''
    Allows bulk insert into models, 
    for large datasets. Inserts by
    batches.
    '''

    def bulk_insert(self, file, model_name, rebuild=False, batch_size=10000, rename_columns={}, exclude=[]) :
        df = pd.read_parquet(file)
        df = df.replace({np.nan : None})
        df = df.drop(columns=exclude)

        if "state" in df.columns:
            df["state"].replace(STATE_ABBR, inplace=True)

        if rename_columns : 
            df.rename(columns = rename_columns, inplace = True)
        
        groups = df.groupby(np.arange(len(df))//batch_size)        
        
        model_choice = apps.get_model("data_gov_my", model_name)

        if rebuild :
            model_choice.objects.all().delete()

        for k,v in groups :
            model_rows = [ model_choice(**i) for i in v.to_dict('records') ]
            model_choice.objects.bulk_create(model_rows)

    '''
    Validates a request,
    by checking if all parameters exists.
    '''
    def is_params_exist(self, request_params) :
        for i in self.required_params :
            if i not in request_params :
                return False
        return True

    '''
    Converts a string into a boolean
    '''
    def str2bool(self, b):
        return b.lower() in ("yes", "true", "t", "1")    