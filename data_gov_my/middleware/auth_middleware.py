import os
from django.http import JsonResponse
from django.core.cache import cache
from data_gov_my.models import AuthTable

class AuthMiddleware:
    
    _exclude = ['UPDATE', 'AUTH_TOKEN', 'FORMS']
    
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if 'Authorization' in request.headers : 
            return self.get_response(request)
        
        return JsonResponse({"status" : 401, "message" : "Middleware Unauthorized"}, status=400)
    
    def process_view(self, request, view_func, view_args, view_kwargs) : 
        view_name = view_func.view_class.__name__
        req_auth_key = request.headers.get("Authorization")

        if view_name not in self._exclude : 
            auth_key = cache.get("AUTH_KEY")
            if not auth_key : 
                auth_key = AuthTable.objects.filter(key="AUTH_TOKEN").values('value').first()['value']
                cache.set("AUTH_KEY", auth_key)
            if req_auth_key != auth_key : 
                return JsonResponse({"status" : 401, "message" : "Middleware Unauthorized"}, status=400)
        else : 
            auth_key = os.getenv("WORKFLOW_TOKEN")
            if auth_key != req_auth_key : 
                return JsonResponse({"status" : 401, "message" : "Middleware Unauthorized"}, status=400)