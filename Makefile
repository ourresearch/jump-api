routes:
	FLASK_APP=views.py flask routes

test:
	heroku local:run pytest -v

ipython:
	heroku local:run ipython

run:
	heroku local:run python views.py
