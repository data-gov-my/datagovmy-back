# Loaded automatically by gunicorn from the working directory.
# Log the real client IP (set by DigitalOcean); %(h)s is only DO's internal proxy.
accesslog = "-"
access_log_format = '%({do-connecting-ip}i)s %(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'
