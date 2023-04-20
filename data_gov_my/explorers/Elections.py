import calendar
from datetime import datetime, timedelta
from typing import List
from data_gov_my.explorers.General import General_Explorer
from rest_framework import exceptions
from django.http import JsonResponse
from django.apps import apps
from data_gov_my.models import DashboardJson
from data_gov_my.serializers import ElectionCandidateSerializer



class ELECTIONS(General_Explorer):

    # General Data    
    explorer_name = "ELECTIONS"

    # List of charts within explorer, with own endpoints
    charts = ["candidates"]

    # List of dropdowns within explorer, with own endpoints
    dropdowns=['candidate_list']

    # Data population
    data_populate = {
        'ElectionDashboard_Candidates' : 'https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/elections_candidates.parquet'
    }

    # API related
    required_params = ['explorer', 'chart']
    param_models = {'candidates' : 'ElectionDashboard_Candidates'}

    '''
    Constructor
    '''
    def __init__(self):
        General_Explorer.__init__(self)
    
    '''
    Dafault API handler
    '''

    # TODO: Handle this better
    def handle_api(self, request_params):
        
        # Handles Dropdowns
        if 'dropdown' in request_params and request_params["dropdown"][0] in self.dropdowns :
            dropdown_type = request_params["dropdown"][0]
            if dropdown_type == 'candidate_list' :
                res = self.candidate_list()
                return JsonResponse(res["msg"], status=res["status"], safe=False)

        # Handles Charts
        if "chart" in request_params and request_params["chart"][0] in self.charts:
            chart_type = request_params["chart"][0]
            if chart_type == "candidates" :
                res = self.candidates_chart(request_params=request_params)
                return JsonResponse(res["msg"], status=res["status"], safe=False)
            
        return JsonResponse({"400" : "Bad Request."}, status=400)


    '''
    Handles Candidate dropdown list
    '''
    def candidate_list(self) :
        model_name = 'ElectionDashboard_Candidates'
        model_choice = apps.get_model('data_gov_my', model_name)
        data = list(model_choice.objects.values_list('name', flat=True).distinct())
        res = {}
        res["msg"] = data
        res["status"] = 200
        return res


    '''
    Handles chart for candidates
    '''
    def candidates_chart(self, request_params) :
        required_params = ["name", "type"] # Declare required params

        res = {}

        if not all(param in request_params for param in required_params) :
            res["msg"] = {"400" : "Bad request"} 
            res["status"] = 400
            return res

        model_name = 'ElectionDashboard_Candidates'
        
        name = request_params['name'][0]
        e_type = request_params['type'][0]

        model_choice = apps.get_model('data_gov_my', model_name)

        candidates_res = model_choice.objects.filter(name=name, type=e_type)
        serializer = ElectionCandidateSerializer(candidates_res, many=True)

        res["msg"] = serializer.data
        res["status"] = 200

        return res