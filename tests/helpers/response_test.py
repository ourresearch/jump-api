import json
import unittest

from jsonschema import validate, ValidationError

from views import app
import os
import urllib.parse
from urllib.parse import urlencode


class ResponseTest(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.client.testing = True

    def json_response(self, url):
        return json.loads(self.client.get(url).get_data(as_text=True))


def assert_schema(obj, schema, test_name):
    try:
        validate(obj, schema)
    except ValidationError as e:
        raise AssertionError(u'error in {}: {}'.format(test_name, str(e)))


def dev_request_url(path, params=None):
    params = params.copy() if params else {}
    params.update({
        'secret': os.getenv("JWT_SECRET_KEY")
    })

    return urllib.parse.urlunparse([
        '',
        '',
        path,
        '',
        urlencode(params),
        '',
    ])
