routes:
	FLASK_APP=views.py flask routes

test:
	heroku local:run python3 -m pytest -v

ipython:
	heroku local:run python3 -m IPython

run:
	heroku local:run python3 views.py
