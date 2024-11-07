import os
from django.http import JsonResponse
from django.core.cache import cache
from data_gov_my.models import AuthTable


class AuthMiddleware:
    # Declares endpoints to exclude, as well as the request methods
    _exclude = {
        "UPDATE": ["POST"],
        "AUTH_TOKEN": ["POST"],
        "UPDATE_VIEW_COUNT": ["POST"],
        "FORMS": ["DELETE"],
    }

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if self.is_admin_panel(request):
            return self.get_response(request)

        if self.is_subscription_related(request):
            return self.get_response(request)

        if "Authorization" in request.headers:
            return self.get_response(request)

        return JsonResponse({"status": 401, "message": "Unauthorized"}, status=400)

    def process_view(self, request, view_func, view_args, view_kwargs):
        if not self.is_admin_panel(request=request) and not self.is_subscription_related(request=request):
            view_name = view_func.view_class.__name__
            req_auth_key = request.headers.get("Authorization")
            master_token = os.getenv("WORKFLOW_TOKEN")
            request_type = request.method

            if (view_name in self._exclude) and (
                    request_type in self._exclude[view_name]
            ):
                if master_token != req_auth_key:
                    return JsonResponse(
                        {"status": 401, "message": "Unauthorized"}, status=400
                    )
            else:
                auth_key = cache.get("AUTH_KEY")
                if not auth_key:
                    auth_key = (
                        AuthTable.objects.filter(key="AUTH_TOKEN")
                        .values("value")
                        .first()["value"]
                    )
                    cache.set("AUTH_KEY", auth_key)
                if (req_auth_key != auth_key) and (req_auth_key != master_token):
                    return JsonResponse(
                        {"status": 401, "message": "Unauthorized"}, status=400
                    )

    def is_subscription_related(self, request):
        if (
                "/token/request/" in request.path_info or
                "/token/verify/" in request.path_info or
                "/subscriptions/" in request.path_info
        ):
            return True
        return False

    def is_admin_panel(self, request):
        if "/admin/" in request.path_info:
            return True
        return False
