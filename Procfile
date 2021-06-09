web: PRELOAD_LARGE_TABLES=True NEW_RELIC_CONFIG_FILE=newrelic.ini newrelic-admin run-program gunicorn -w 1 views:app --reload
parse_uploads: python parse_uploads.py
consortium_calculate: python consortium_calculate.py
warm_cache: python warm_cache.py
