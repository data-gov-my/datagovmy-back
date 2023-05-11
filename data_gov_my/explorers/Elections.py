import calendar
from datetime import datetime, timedelta
from typing import List
from data_gov_my.explorers.General import General_Explorer
from rest_framework import exceptions
from django.http import JsonResponse
from django.apps import apps
from data_gov_my.models import DashboardJson
from data_gov_my.serializers import ElectionCandidateSerializer, ElectionSeatSerializer, ElectionPartySerializer, ElectionOverallSeatSerializer



class ELECTIONS(General_Explorer):

    # General Data    
    explorer_name = "ELECTIONS"

    # List of charts within explorer, with own endpoints
    charts = ["candidates", "seats", "party", "full_result", "overall_seat"]

    # List of dropdowns within explorer, with own endpoints
    dropdowns=['candidate_list', "seats_list", "party_list"]

    # Data population
    data_populate = {
        'ElectionDashboard_Candidates' : 'https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/elections_candidates.parquet',
        'ElectionDashboard_Seats' : 'https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/elections_seats_winner.parquet',
        'ElectionDashboard_Party' : 'https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/elections_parties.parquet'
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

    # TODO: Handle this better
    def handle_api(self, request_params):
        
        # Handles Dropdowns
        if 'dropdown' in request_params and request_params["dropdown"][0] in self.dropdowns :
            dropdown_type = request_params["dropdown"][0]
            if dropdown_type == 'candidate_list' :
                res = self.candidate_list()
                return JsonResponse(res["msg"], status=res["status"], safe=False)
            if dropdown_type == 'seats_list' : 
                res = self.seat_list()
                return JsonResponse(res["msg"], status=res["status"], safe=False)
            if dropdown_type == 'party_list' : 
                res = self.party_list()
                return JsonResponse(res["msg"], status=res["status"], safe=False)

        # Handles Charts
        if "chart" in request_params and request_params["chart"][0] in self.charts:
            chart_type = request_params["chart"][0]
            if chart_type == "candidates" :
                res = self.candidates_chart(request_params=request_params)
                return JsonResponse(res["msg"], status=res["status"], safe=False)
            if chart_type == "seats" :
                res = self.seats_chart(request_params=request_params)
                return JsonResponse(res["msg"], status=res["status"], safe=False)
            if chart_type == "party" :
                res = self.party_chart(request_params=request_params)
                return JsonResponse(res["msg"], status=res["status"], safe=False)
            if chart_type == "full_result" :
                res = self.full_result(request_params=request_params)
                return JsonResponse(res["msg"], status=res["status"], safe=False)
            if chart_type == "overall_seat" :
                res = self.overall_seat(request_params=request_params)
                return JsonResponse(res["msg"], status=res["status"], safe=False)

        return JsonResponse({"400" : "Bad Request."}, status=400)

    def full_result(self, request_params) :
        required_params = ["type"] # Declare required params

        res = {}

        if not all(param in request_params for param in required_params) :
            res["msg"] = {"400" : "Bad request"} 
            res["status"] = 400
            return res
        
        c_type = request_params['type'][0]
        model_name = ''
        c_serializer = ''
        c_filters = {}
        extract_votes_outcome = False

        if c_type == "candidates" or c_type == "seats":
            election = request_params['election'][0]
            seat = request_params['seat'][0]
            model_name = 'ElectionDashboard_Candidates'
            c_serializer = ElectionCandidateSerializer
            c_filters = {"election_name" : election, "seat" : seat}
            extract_votes_outcome = True
        if c_type == "party" : 
            election = request_params['election'][0]
            state = request_params['state'][0]
            model_name = "ElectionDashboard_Party"
            c_serializer = ElectionPartySerializer
            c_filters = {"election_name":election,'state':state}


        model_choice = apps.get_model('data_gov_my', model_name)
        candidates_res = model_choice.objects.filter(**c_filters)
        data = c_serializer(candidates_res, many=True).data

        # For particular case of full_results
        if extract_votes_outcome and len(data) > 0 :
            const_keys = ["voter_turnout", "voter_turnout_perc", "votes_rejected", "votes_rejected_perc"]
            r = {}
            r["votes"] = {k: data[0][k] for k in const_keys}
            r["data"] = [{k: v for k, v in d.items() if k not in const_keys} for d in data]
            data = r

        res["msg"] = data
        res["status"] = 200

        return res




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
    Handles Seat dropdown list
    '''
    def seat_list(self) :
        model_name = 'ElectionDashboard_Seats'
        model_choice = apps.get_model('data_gov_my', model_name)
        data = list(model_choice.objects.values('seat_name', 'type').distinct())
        res = {}
        res["msg"] = data
        res["status"] = 200
        return res

    '''
    Handles Party dropdown list
    '''
    def party_list(self) :
        model_name = 'ElectionDashboard_Party'
        model_choice = apps.get_model('data_gov_my', model_name)
        data = list(model_choice.objects.values_list('party', flat=True).distinct())
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
    
    '''
    Handles Seats charts
    '''
    def seats_chart(self, request_params) :
        required_params = ["seat_name"]

        res = {}
        print(request_params)
        if not all(param in request_params for param in required_params) :
            res["msg"] = {"400" : "Bad request"} 
            res["status"] = 400
            return res
    
        model_name = 'ElectionDashboard_Seats'
        
        name = request_params['seat_name'][0]
        model_choice = apps.get_model('data_gov_my', model_name)

        candidates_res = model_choice.objects.filter(seat_name=name)
        serializer = ElectionSeatSerializer(candidates_res, many=True)

        res["msg"] = serializer.data
        res["status"] = 200

        return res
    
    '''
    Handles Party Charts
    '''
    def party_chart(self, request_params) : 
        required_params = ["party_name", "state", "type"]

        res = {}
        print(request_params)
        if not all(param in request_params for param in required_params) :
            res["msg"] = {"400" : "Bad request"} 
            res["status"] = 400
            return res
    
        model_name = 'ElectionDashboard_Party'
        
        name = request_params['party_name'][0]
        state = request_params['state'][0]
        area_type = request_params['type'][0]
        model_choice = apps.get_model('data_gov_my', model_name)

        candidates_res = model_choice.objects.filter(party=name, state=state, type=area_type)
        serializer = ElectionPartySerializer(candidates_res, many=True)

        res["msg"] = serializer.data
        res["status"] = 200

        return res
    
    '''
    Handles Overall Seats Charts
    '''
    def overall_seat(self, request_params) : 
        required_params = ["state", "type", "election"]

        res = {}
        
        if not all(param in request_params for param in required_params) :
            res["msg"] = {"400" : "Bad request"} 
            res["status"] = 400
            return res
    
        model_name = 'ElectionDashboard_Seats'
        
        state = request_params['state'][0]
        area_type = request_params['type'][0]
        election = request_params['election'][0]
        
        model_choice = apps.get_model('data_gov_my', model_name)

        overall_res = None

        if area_type == 'parlimen' and state == 'mys' : 
            overall_res = model_choice.objects.filter(election_name=election, type=area_type)
        else : 
            overall_res = model_choice.objects.filter(election_name=election, state=state, type=area_type)
        
        serializer = ElectionOverallSeatSerializer(overall_res, many=True)

        res["msg"] = serializer.data
        res["status"] = 200

        return res