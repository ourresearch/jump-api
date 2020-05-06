from response_test import ResponseTest, dev_request_url, assert_schema
from saved_scenario import SavedScenario
from test_package import packages_to_check
from views import app
from test_journal import journal_to_dict_journals_schema
from test_journal import journal_to_dict_raw_schema

scenarios_to_check = [
    s for s in [
        SavedScenario.query.filter(SavedScenario.package_id == p.package_id).first() for p in packages_to_check if p
    ] if s
]

scenario_meta_schema = {
    'type': 'object',

    'required': [
        'scenario_id',
        'scenario_name',
        'publisher_id',
        'publisher_name',
        'institution_id',
        'institution_name',
        'scenario_created',
        'is_base_scenario',
    ],

    'properties': {
        'scenario_id': {'type': 'string'},
        'scenario_name': {'type': ['string', 'null']},
        'publisher_id': {'type': 'string'},
        'publisher_name': {'type': 'string'},
        'institution_id': {'type': 'string'},
        'institution_name': {'type': 'string'},
        'scenario_created': {'type': 'string'},
        'is_base_scenario': {'type': 'boolean'},
    },

    'additionalProperties': False,
}

scenario_settings = {
    'type': 'object',

    'required': [
        'cost_bigdeal',
        'include_social_networks',
        'include_bronze',
        'include_submitted_version',
        'cost_ill',
        'cost_bigdeal_increase',
        'cost_content_fee_percent',
        'ill_request_percent_of_delayed',
        'cost_alacart_increase',
        'backfile_contribution',
        'weight_authorship',
        'weight_citation',
        'include_backfile',
    ],

    'properties': {
        'cost_bigdeal': {'type': 'number'},
        'include_social_networks': {'type': 'boolean'},
        'include_bronze': {'type': 'boolean'},
        'include_submitted_version': {'type': 'boolean'},
        'cost_ill': {'type': 'number'},
        'cost_bigdeal_increase': {'type': 'number'},
        'cost_content_fee_percent': {'type': 'number'},
        'ill_request_percent_of_delayed': {'type': 'number'},
        'cost_alacart_increase': {'type': 'number'},
        'backfile_contribution': {'type': 'number'},
        'weight_authorship': {'type': 'number'},
        'weight_citation': {'type': 'number'},
        'include_backfile': {'type': 'boolean'},
        'package': {'type': 'string'},
    },

    'additionalProperties': False,
}

scenario_saved_schema = {
    'type': 'object',

    'required': [
        'subrs',
        'customSubrs',
        'configs',
        'name',
        'id',
    ],

    'properties': {
        'subrs': {'type': 'array', 'items': {'type': 'string'}},
        'customSubrs': {'type': 'array', 'items': {'type': 'string'}},
        'configs': scenario_settings,
        'name': {'type': 'string'},
        'id': {'type': 'string'},
    },

    'additionalProperties': False,
}


class TestScenario(ResponseTest):
    def test_scenario(self):
        pass

    def test_scenario_journals(self):
        schema = {
            'type': 'object',

            'required': [
                'meta',
                'saved',
                'journals',
            ],

            'properties': {
                'meta': scenario_meta_schema,
                'saved': scenario_saved_schema,
                'journals': {'type': 'array', 'items': journal_to_dict_journals_schema},
            }
        }

        with app.app_context():
            for test_scenario in scenarios_to_check:
                url = dev_request_url('scenario/{}/journals'.format(test_scenario.scenario_id))
                response = self.json_response(url)

                test_name = u'{} ({}, from package {})'.format(
                    test_scenario.scenario_name,
                    test_scenario.scenario_id,
                    test_scenario.package_id
                )

                assert_schema(response, schema, test_name)

    def test_scenario_raw(self):
        schema = {
            'type': 'object',

            'required': [
                'journals',
            ],

            'properties': {
                'journals': {'type': 'array', 'items': journal_to_dict_raw_schema},
            }
        }

        with app.app_context():
            for test_scenario in scenarios_to_check:
                url = dev_request_url('scenario/{}/raw'.format(test_scenario.scenario_id))
                response = self.json_response(url)

                test_name = u'{} ({}, from package {})'.format(
                    test_scenario.scenario_name,
                    test_scenario.scenario_id,
                    test_scenario.package_id
                )

                assert_schema(response, schema, test_name)

    def test_scenario_slider(self):
        pass

    def test_scenario_subscriptions(self):
        pass

    def test_scenario_table(self):
        pass

    def test_scenario_export(self):
        pass