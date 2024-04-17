from rest_framework.throttling import AnonRateThrottle


class FormRateThrottle(AnonRateThrottle):
    rate = "4/min"
