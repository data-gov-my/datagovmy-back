import calendar
from datetime import datetime, timedelta
from typing import List
from data_gov_my.explorers.General import General_Explorer
from rest_framework import exceptions
from django.http import JsonResponse
from django.apps import apps
from data_gov_my.serializers import ElectionCandidateSerializer, ElectionSeatSerializer, ElectionPartySerializer, ElectionOverallSeatSerializer
from collections import defaultdict


class ELECTIONS(General_Explorer):

    # General Data    
    explorer_name = "ELECTIONS"

    # List of charts within explorer, with own endpoints
    charts = ["candidates", "seats", "party", "full_result", "overall_seat"]

    # List of dropdowns within explorer, with own endpoints
    dropdowns = {
        'candidate_list' : {
            "model" : 'ElectionDashboard_Candidates',
            "values" : ['name'],
            "flat" : True
        },
        'seats_list' : {
            "model" : 'ElectionDashboard_Seats',
            "values" : ['seat_name', 'type'],
            "flat" : False
        },
        'party_list' : {
            "model" : 'ElectionDashboard_Party',
            "values" : ['party'],
            "flat" : True
        },
        'election_list' : {
            "model" : 'ElectionDashboard_Dropdown',
            "values" : ['state', 'election', 'date'],
            "flat" : False
        }
    }

    chart_meta = {
        "party" : {
            "model_name" : 'ElectionDashboard_Party',
            "serializer" : ElectionPartySerializer,
            "segment_by" : 'type',
            "params_mapping" : {
                "party" : "party_name",
                "state" : "state"
            }
        },
        "seats" : {
            "model_name" : 'ElectionDashboard_Seats',
            "serializer" : ElectionSeatSerializer,
            "segment_by" : None,
            "params_mapping" : {
                "slug" : "seat_name",
                "type" : "type"
            }
        },
        "candidates" : {
            "model_name" : 'ElectionDashboard_Candidates',
            "serializer" : ElectionCandidateSerializer,
            "segment_by" : 'type',
            "params_mapping" : {
                "slug" : "name"
            }
        }
    }


    # Data population
    data_populate = {
        'ElectionDashboard_Candidates' : 'https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/elections_candidates.parquet',
        'ElectionDashboard_Seats' : 'https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/elections_seats_winner.parquet',
        'ElectionDashboard_Party' : 'https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/elections_parties.parquet',
        'ElectionDashboard_Dropdown' : 'https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/elections_dates.parquet'
    }

    # API related
    required_params = ['explorer', 'chart']
    param_models = {
        'candidates' : 'ElectionDashboard_Candidates', 
        'seats' : 'ElectionDashboard_Seats',
        'party' : 'ElectionDashboard_Party'
    }

    '''
    Constructor
    '''
    def __init__(self):
        General_Explorer.__init__(self)
    
    '''
    Dafault API handler
    '''

    def handle_api(self, request_params):
        
        # Handles Dropdowns
        if 'dropdown' in request_params and request_params["dropdown"][0] in self.dropdowns :
            dropdown_type = request_params["dropdown"][0]
            res = self.get_dropdown(dropdown_type=dropdown_type)
            return JsonResponse(res["msg"], status=res["status"], safe=False)

        # Handles Charts
        if "chart" in request_params and request_params["chart"][0] in self.charts:
            chart_type = request_params["chart"][0]
            res = {}

            if chart_type == "full_result" :
                res = self.full_result(request_params=request_params)
            elif chart_type == "overall_seat" :
                res = self.overall_seat(request_params=request_params)
            else :
                res = self.chart_handler(request_params=request_params, chart=chart_type)

            return JsonResponse(res["msg"], status=res["status"], safe=False)
            

        return JsonResponse({"400" : "Bad Request."}, status=400)

    '''
    Multi-handler dropdown type
    '''

    def get_dropdown(self, dropdown_type='') :
        model_name = self.dropdowns[dropdown_type]['model']
        values = self.dropdowns[dropdown_type]['values']
        flat = self.dropdowns[dropdown_type]['flat']
        model_choice = apps.get_model('data_gov_my', model_name)
        data = None

        if dropdown_type == 'seats_list' : 
            data = list(model_choice.objects.values(*values).distinct())
        elif dropdown_type == 'election_list' :
            data = defaultdict(list)
            for x in list(model_choice.objects.values()) :
                data[x.get('state')].append({ 'name' : x['election'], 'year' : str(x['date']) })
        else : 
            data = list(model_choice.objects.values_list(*values, flat=flat).distinct())

        res = {}
        res["msg"] = data
        res["status"] = 200
        return res
   
    '''
    Handles general charts :
    - Seats
    - Candidates
    - Parties
    '''
    def chart_handler(self, request_params, chart) :        
        chart_choice = self.chart_meta[chart]
        required_params = list(chart_choice["params_mapping"].values())
        
        res = {}
        if not all(param in request_params for param in required_params) :
            res["msg"] = {"400" : "Bad request"} 
            res["status"] = 400
            return res

        filters = {k : request_params[v][0] for k, v in chart_choice["params_mapping"].items()}
        model_choice = apps.get_model('data_gov_my', chart_choice["model_name"])
        db_data = model_choice.objects.filter(**filters)
        c_serializer = chart_choice["serializer"]
        ser_data = c_serializer(db_data, many=True).data

        if chart_choice["segment_by"] :
            ser_data = self.group_by_type(type=chart_choice["segment_by"], data=ser_data)

        last_update = self.get_last_update(model_name=chart_choice['model_name'])

        r_data = {"data_last_update" : last_update, "data" : ser_data } 
        res["msg"] = r_data
        res["status"] = 200
        return res


    '''
    Segments data by type
    '''
    def group_by_type(self, type="", data=[]) :
        result = {"parlimen" : [], "dun" : []}
        for d in data:
            result[d.get(type)].append(d)
        return result

    '''
    Handles Overall Seats Charts
    '''
    def overall_seat(self, request_params) : 
        required_params = ["state", "election"]

        res = {}
        
        if not all(param in request_params for param in required_params) :
            res["msg"] = {"400" : "Bad request"} 
            res["status"] = 400
            return res
    
        model_name = 'ElectionDashboard_Seats'
        
        state = request_params['state'][0]
        election = request_params['election'][0]
        
        model_choice = apps.get_model('data_gov_my', model_name)

        args = {'election_name' : election}

        if state != "mys" : 
            args['state'] = state

        overall_res = model_choice.objects.filter(**args)
        
        serializer = ElectionOverallSeatSerializer(overall_res, many=True)
        last_update = self.get_last_update(model_name=model_name)

        r_data = {"data_last_update" : last_update, "data" : serializer.data } 
        res["msg"] = r_data
        res["status"] = 200

        return res
    
    '''
    Returns the full result
    '''
    def full_result(self, request_params) :
        required_params = ["type"] # Declare required params

        res = {}

        if not all(param in request_params for param in required_params) :
            res["msg"] = {"400" : "Bad request"} 
            res["status"] = 400
            return res

        c_type = request_params['type'][0] # Get type

        if c_type != "candidates" and c_type != "party" :
            res["msg"] = {"400" : "Bad request"} 
            res["status"] = 400
            return res

        model_name = f'ElectionDashboard_{c_type.capitalize()}'
        extract_votes_outcome = (c_type == 'candidates')
        c_serializer = ElectionCandidateSerializer if extract_votes_outcome else ElectionPartySerializer
        election = request_params['election'][0]
        c_filters = {"election_name" : election}
        req_param = 'seat' if extract_votes_outcome else 'state'
        c_filters[req_param] = request_params[req_param][0]

        model_choice = apps.get_model('data_gov_my', model_name)
        candidates_res = model_choice.objects.filter(**c_filters)
        data = c_serializer(candidates_res, many=True).data

        # For particular case of full_results
        if extract_votes_outcome and len(data) > 0 :
            const_keys = ["voter_turnout", "voter_turnout_perc", "votes_rejected", "votes_rejected_perc", "majority", "majority_perc"]
            r = {}
            r["votes"] = {k: data[0][k] for k in const_keys}
            r["data"] = [{k: v for k, v in d.items() if k not in const_keys} for d in data]
            data = r

        last_update = self.get_last_update(model_name=model_name)

        r_data = {"data_last_update" : last_update, "data" : data } 
        res["msg"] = r_data
        res["status"] = 200

        return res