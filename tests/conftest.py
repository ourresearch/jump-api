import os
import sys
import pytest
import urllib.parse

LIB_DIR = os.path.join('lib', os.path.dirname('.'))
sys.path.insert(0, os.path.abspath(LIB_DIR))
sys.path.append(os.path.join(os.path.dirname(__file__), 'helpers'))

def pytest_report_header(config):
	DATABASE_URL = os.getenv("DATABASE_URL_REDSHIFT_TEST") if os.getenv("TESTING_DB") else os.getenv("DATABASE_URL_REDSHIFT")
	redshift_url = urllib.parse.urlparse(DATABASE_URL)
	return  "hostname: " + redshift_url.hostname
