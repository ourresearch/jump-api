import json
import os
import time
import urlparse
from urllib import urlencode

import requests
from deepdiff import DeepDiff

from package import Package
from views import app
from saved_scenario import SavedScenario


#  heroku local:run nosetests test/test_response_diffs.py


def live_request_url(path, params=None):
    params = params.copy() if params else {}
    params.update({'secret': os.getenv("JWT_SECRET_KEY")})

    return urlparse.urlunparse([
        u'https',
        u'unpaywall-jump-api.herokuapp.com',
        path,
        None,
        urlencode(params),
        None,
    ])


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


def institutions_to_test():
    return [
        'institution-2h4YWhLmGQSy'  # suny
        'institution-iRFuoxgDGH5z',  # suny poly
        'institution-ZPY8mUmpK2gM',  # suny a
        'institution-xFFDfqtaBXik',  # uva
    ]


def strip_timing(nested_iterable):
    if isinstance(nested_iterable, dict):
        nested_iterable.pop('timing', None)
        nested_iterable.pop('_timing', None)

        for i in nested_iterable.values():
            strip_timing(i)

    elif isinstance(nested_iterable, list):
        for i in nested_iterable:
            strip_timing(i)

    return nested_iterable


class ResponseTest(object):
    @classmethod
    def routes(cls):
        raise NotImplementedError()

    @classmethod
    def route_params(cls):
        raise NotImplementedError()

    def generate_tests(self):
        for route in self.routes():
            for institution_id in self.route_params():
                yield self.check_route, route, institution_id

    def check_route(self, route, route_params):
        live_url = live_request_url(route.format(**route_params))
        live_response = strip_timing(requests.get(live_url).json())

        with app.test_client() as test_client:
            test_client.testing = True
            with app.app_context():
                dev_url = dev_request_url(route.format(**route_params))
                dev_response = strip_timing(json.loads(test_client.get(dev_url).get_data(as_text=True)))

        diff = DeepDiff(live_response, dev_response, ignore_order=True)
        diff_json = json.dumps(diff, indent=4)

        if diff_json != '{}':
            print diff_json
            assert False


class TestInstitutionResponses(ResponseTest):
    @classmethod
    def routes(cls):
        return [
            '/institution/{institution_id}',
        ]

    @classmethod
    def route_params(cls):
        return [{'institution_id': x} for x in institutions_to_test()]


class TestPublisherResponses(ResponseTest):
    @classmethod
    def routes(cls):
        return [
            '/live/data/common/{publisher_id}',
            #'/package/{publisher_id}/scenario',
            # '/package/{publisher_id}/scenario?copy=:scenario_id',
            # '/package/{publisher_id}/counter/diff_no_price',
            # '/package/{publisher_id}/counter/no_price',
            '/publisher/{publisher_id}',
            '/publisher/{publisher_id}/apc',
            # '/publisher/{publisher_id}/counter/diff_no_price',
            # '/publisher/{publisher_id}/counter/no_price',
        ]

    @classmethod
    def route_params(cls):
        with app.app_context():
            packages = [Package.query.filter(Package.institution_id == i).first() for i in institutions_to_test()]
        return [{'publisher_id': p.package_id} for p in packages if p]


class TestScenarioResponses(ResponseTest):
    @classmethod
    def routes(cls):
        return [
            '/scenario/{scenario_id}',
            '/scenario/{scenario_id}/journals',
            '/scenario/{scenario_id}/raw',
            '/scenario/{scenario_id}/slider',
            # '/scenario/{scenario_id}/subscriptions',
            '/scenario/{scenario_id}/table',
            # '/scenario/{scenario_id}/export.csv?jwt=:jwt',
        ]

    @classmethod
    def route_params(cls):
        with app.app_context():
            packages = [Package.query.filter(Package.institution_id == i).first() for i in institutions_to_test()]
            scenarios = [SavedScenario.query.filter(SavedScenario.package_id == p.package_id).first() for p in packages if p]

        return [{'scenario_id': s.scenario_id} for s in scenarios if s]

journal_routes = [
    '/scenario/{scenario_id}/journal/:issn_l',
]

user_routes = [
    '/user/demo',
    '/user/login',
    '/user/me',
    '/user/new',
]