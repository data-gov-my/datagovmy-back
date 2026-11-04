from rest_framework.throttling import AnonRateThrottle


class FormRateThrottle(AnonRateThrottle):
    rate = "4/min"

    def get_ident(self, request):
        # DigitalOcean sets this to the real client IP and overwrites any client-sent value.
        # DRF's default reads X-Forwarded-For, which the client can spoof to dodge the limit.
        return request.META.get("HTTP_DO_CONNECTING_IP") or super().get_ident(request)
