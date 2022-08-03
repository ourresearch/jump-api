web: gunicorn -w 6 views:app --reload
parse_uploads: python parse_uploads.py
consortium_calculate: python consortium_calculate.py
warm_cache: python warm_cache.py
worker: celery -A tasks.celery worker --loglevel=WARNING --concurrency=2
