web: PRELOAD_LARGE_TABLES=True NEW_RELIC_CONFIG_FILE=newrelic.ini newrelic-admin run-program gunicorn -w 4 views:app --reload
consortium_calculate: python consortium_calculate.py
warm_cache: python warm_cache.py
