import json
import os
import time
import unittest
import urlparse
from urllib import urlencode

from jsonschema import validate, ValidationError

from views import app


#  heroku local:run nosetests test/test_institution.py


def dev_request_url(path, params=None):
    params = params.copy() if params else {}
    params.update({
        'secret': os.getenv("JWT_SECRET_KEY"),
        'cache_breaker': int(time.time())
    })

    return urlparse.urlunparse([
        None,
        None,
        path,
        None,
        urlencode(params),
        None,
    ])


def institutions_to_check():
    return {
        'suny': 'institution-2h4YWhLmGQSy',
        'suny_poly': 'institution-iRFuoxgDGH5z',
        'suny_a': 'institution-ZPY8mUmpK2gM',
        'uva': 'institution-xFFDfqtaBXik',
    }


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


class TestInstitution(ResponseTest):
    def test_institutions(self):
        for institution_name, institution_id in institutions_to_check().items():
            url = dev_request_url('/institution/{}'.format(institution_id))
            response = self.json_response(url)

            schema = {
                'definitions': {
                    'user_permissions': {
                        'type': 'object',

                        'required': [
                            'username',
                            'institution_id',
                            'is_authenticated_user',
                            'permissions',
                            'user_name',
                            'user_id',
                            'institution_name',
                            'user_email',
                        ],

                        'properties': {
                            'username': {'type': ['string', 'null']},
                            'institution_id': {'type': 'string'},
                            'is_authenticated_user': {'type': 'boolean'},
                            'permissions': {
                                'type': 'array',
                                'items': {'type': 'string'}
                            },
                            'user_name': {'type': ['string', 'null']},
                            'user_id': {'type': 'string'},
                            'institution_name': {'type': 'string'},
                            'user_email': {'type': ['string', 'null']},
                        }
                    },

                    'publisher': {
                        'type': 'object',

                        'required': [
                            'name',
                            'id',
                        ],

                        'properties': {
                            'id': {'type': 'string'},
                            'name': {'type': ['string', 'null']},
                        }
                    }
                },

                'type': 'object',

                'required': [
                    'id',
                    'ror_ids',
                    'name',
                    'user_permissions',
                    'publishers',
                    'grid_ids',
                    'is_demo',
                ],

                'additionalProperties': True,

                'properties': {
                    'id': {'type': 'string'},
                    'ror_ids': {
                        'type': 'array',
                        'items': {'type': 'string'}
                    },
                    'name': {'type': 'string'},
                    'user_permissions': {
                        'type': 'array',
                        'items': {'$ref': '#/definitions/user_permissions'}
                    },
                    'publishers': {
                        'type': 'array',
                        'items': {'$ref': '#/definitions/publisher'}
                    },
                    'grid_ids': {
                        'type': 'array',
                        'items': {'type': 'string'}
                    },
                    'is_demo': {'type': 'boolean'},
                }
            }

            assert_schema(response, schema, institution_name)

#
#
# class TestPublisherResponses(ResponseTest):
#     @classmethod
#     def routes(cls):
#         return [
#             '/live/data/common/{publisher_id}',
#             #'/package/{publisher_id}/scenario',
#             # '/package/{publisher_id}/scenario?copy=:scenario_id',
#             # '/package/{publisher_id}/counter/diff_no_price',
#             # '/package/{publisher_id}/counter/no_price',
#             '/publisher/{publisher_id}',
#             '/publisher/{publisher_id}/apc',
#             # '/publisher/{publisher_id}/counter/diff_no_price',
#             # '/publisher/{publisher_id}/counter/no_price',
#         ]
#
#     @classmethod
#     def route_params(cls):
#         with app.app_context():
#             packages = [Package.query.filter(Package.institution_id == i).first() for i in institutions_to_test()]
#         return [{'publisher_id': p.package_id} for p in packages if p]
#
#
# class TestScenarioResponses(ResponseTest):
#     @classmethod
#     def routes(cls):
#         return [
#             #'/scenario/{scenario_id}',
#             '/scenario/{scenario_id}/journals',
#             #'/scenario/{scenario_id}/raw',
#             #'/scenario/{scenario_id}/slider',
#             # '/scenario/{scenario_id}/subscriptions',
#             #'/scenario/{scenario_id}/table',
#             # '/scenario/{scenario_id}/export.csv?jwt=:jwt',
#         ]
#
#     @classmethod
#     def route_params(cls):
#         with app.app_context():
#             packages = [Package.query.filter(Package.institution_id == i).first() for i in institutions_to_test()]
#             scenarios = [SavedScenario.query.filter(SavedScenario.package_id == p.package_id).first() for p in packages if p]
#
#         return [{'scenario_id': s.scenario_id} for s in scenarios if s]
#
# journal_routes = [
#     '/scenario/{scenario_id}/journal/:issn_l',
# ]
#
# user_routes = [
#     '/user/demo',
#     '/user/login',
#     '/user/me',
#     '/user/new',
# ]