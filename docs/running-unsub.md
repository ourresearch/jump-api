# Running the Unsub backend ("jump-api")

## Running Unsub locally

### With local backend

```
cd jump-api
make run
```

Then spin up the front end:

```
cd get-unsub
npm run serve -- --port 8081
```

Port 8081 will use the localhost base URL (`http://localhost:5004/`)


### With backend running on Heroku staging app

First, turn on the test Redshift database:

- Go to Amazon Redshift, login if needed
- Go to Clusters
- Select "ricks-test"
- In the Actions drop-down select "Resume"

Next, make sure Heroku staging is turned on and using whatever state of the code you want running on staging:

- Login at Heroku website
- Go to app `staging-jump-api`
- Go to Resources
- Turn on "web" and "parse_uploads"
	- Press the pencil icon for each, and use 1 dyno for each

Then spin up front end with port 8082:

```
cd get-unsub
npm run serve -- --port 8082
```

Port 8082 will use the Heroku staging base URL (`https://staging-jump-api.herokuapp.com`)

Running staging on Heroku is completely separate from Unsub in production. That is, on Heroku staging, the environment variable `TESTING_DB` is set to `True` (`TESTING_DB` env var is not set at all on Unsub production on Heroku). If the env var `TESTING_DB=True` is found, a number of points in this codebase change what resources are used:

1. `app.py`: `TESTING_DB=True` sets the base URL to the value of the env var `DATABASE_URL_REDSHIFT_TEST` instead of `DATABASE_URL_REDSHIFT`
2. `package.py`: `TESTING_DB=True` sets `unsub-file-uploads-preprocess-testing` instead of `unsub-file-uploads-preprocess` (the S3 bucket where user file uploads end up; this bucket always empty except for the few moments between `parse_uploads.py` cycles)
3. `package_input.py`: `TESTING_DB=True` sets `unsub-file-uploads-testing` instead of `unsub-file-uploads` (the S3 bucket where we deposit files after we process them)
4. `parse_uploads.py`: `TESTING_DB=True` sets `unsub-file-uploads-preprocess-testing` instead of `unsub-file-uploads-preprocess` and sets `unsub-file-uploads-testing` instead of `unsub-file-uploads` (see description in 2 and 3 for these buckets)
5. `views.py`: `TESTING_DB=True` sets `unsub-file-uploads-preprocess-testing` instead of `unsub-file-uploads-preprocess` (so that user file uploads go to the testing preprocess bucket instead of the production preprocess bucket)


MAKE SURE TO TURN OFF THE TEST REDSHIFT DATABASE WHEN YOU'RE DONE!

- Go to Amazon Redshift, login if needed
- Go to Clusters
- Select "ricks-test"
- In the Actions drop-down select "Pause"

## Runnig tests

To run tests against the test Redshift database and staging Heroku API, prepend any command line calls with the `TESTING_DB` env var. For example, for one test file or test within a file:

```
TESTING_DB=true pytest -v tests/test_counter_input.py
TESTING_DB=true pytest -v tests/test_counter_input.py -k 'test_imports_counter_4_samples'
```

Or all tests

```
TESTING_DB=true pytest -v
```

## Interactive coding

When running Python interactively, make sure to use the `TESTING_DB` env var when you drop into python, e.g.:

```python3
TESTING_DB=true make ipython
TESTING_DB=true python
TESTING_DB=true ipython
```

# Celery and Redis

This is used only for updating the table `jump_apc_authorships` - for some yet unknown reason this takes a long time, e.g. 30 seconds to a minute or two even. This works fine locally, but on Heroku you hit the 30 sec timeout and the request fails.

When running on production on Heroku, the `REDIS_URL` env is set by Heroku from the Heroku add on for Redis (`heroku-redis`)

When running locally, spin up redis like `redis-server`, then start up jump-api setting the env var for `REDIS_URL` to the local URI for your Redis instance, then spin up a celery worker:

```
REDIS_URL=redis://localhost:6379 make run
REDIS_URL=redis://localhost:6379 celery -A tasks.celery worker --loglevel=WARNING
```

## Logs for Celery worker

```
heroku logs --tail --app unpaywall-jump-api --dyno worker.1
```
