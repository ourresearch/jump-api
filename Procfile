web: PRELOAD_LARGE_TABLES=True NEW_RELIC_CONFIG_FILE=newrelic.ini newrelic-admin run-program gunicorn views:app -w 2 --timeout 36000 --reload
warm_cache: python warm_cache.py
