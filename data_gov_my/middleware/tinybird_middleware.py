import datetime

from django.conf import settings
from django.http import HttpRequest
from django.urls import resolve

from data_gov_my.tasks import post_api_log_to_tinybird


class TinyBirdAPILoggerMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response
        # One-time configuration and initialization.

        self.TINYBIRD_API_LOGGER_ENABLED = True
        if hasattr(settings, "TINYBIRD_API_LOGGER_ENABLED"):
            self.TINYBIRD_API_LOGGER_ENABLED = settings.TINYBIRD_API_LOGGER_ENABLED

    def __call__(self, request: HttpRequest):
        if not self.TINYBIRD_API_LOGGER_ENABLED:
            return self.get_response(request)

        namespace = resolve(request.path_info).namespace

        response = self.get_response(request)

        # skip admin panel
        if namespace == "admin":
            return response

        tinybird_payload = dict(
            timestamp=datetime.datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            ),  # datetime format on tinybird
            url=request.path_info,
            # params=dict(request.GET),
            method=request.method,
            status_code=response.status_code,
            client_ip=request.META.get("REMOTE_ADDR", ""),
            session_key=request.session.session_key or "",
            # headers=dict(request.headers),
            http_host=request.META.get("HTTP_HOST", ""),
            # http_referer=request.META.get("HTTP_REFERER"),
            http_user_agent=request.META.get("HTTP_USER_AGENT", ""),
            query_string=request.META.get("QUERY_STRING", ""),
            # remote_addr=request.META.get("REMOTE_ADDR", ""),
        )

        post_api_log_to_tinybird.delay(tinybird_payload)

        return response
