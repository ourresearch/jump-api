# long web timeout value needed to facilitate proxy of s3 changefile content
# setting to 10 hours: 60*60*10=36000
# web: gunicorn views:app -w 5 --timeout 36000 --reload
web: NEW_RELIC_CONFIG_FILE=newrelic.ini newrelic-admin run-program gunicorn views:app -w 4 --timeout 36000 --reload


