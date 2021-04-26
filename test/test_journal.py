from response_test import ResponseTest, dev_request_url, assert_schema
from views import app

era_subject_schema = {
    'type': 'array',

    'items': [
        {
            'type': 'string',
            'minLength': 2,
            'maxLength': 4,
        },
        {
            'type': 'string',
        },
    ],
}

journal_to_dict_journals_schema = {
    'type': 'object',

    'required': [
        'issn_l',
        'title',
        'subject',
        'era_subjects',
        'subscribed',
        'is_society_journal',
        'use_total',
        'cost_subscription',
        'cost_ill',
        'use_instant',
        'use_instant_percent',
        'use_groups_free_instant',
        'use_groups_if_subscribed',
        'use_groups_if_not_subscribed',
        'ncppu',
        'ncppu_rank',
        'cost',
        'usage',
        'instant_usage_percent',
        'free_instant_usage_percent',
        'subscription_cost',
        'ill_cost',
        'subscription_minus_ill_cost',
        'use_oa_percent',
        'use_backfile_percent',
        'use_subscription_percent',
        'use_ill_percent',
        'use_other_delayed_percent',
        'perpetual_access_years_text',
        'use_asns_percent',
        'use_green_percent',
        'use_hybrid_percent',
        'use_bronze_percent',
        'use_peer_reviewed_percent',
        'oa_embargo_months',
        'is_hybrid_2019',
        'downloads',
        'citations',
        'authorships',
    ],

    'properties': {
        'issn_l': {'type': ['null', 'string']},
        'title': {'type': 'string'},
        'subject': {'type': ['string', 'null']},
        'era_subjects': {
            'type': 'array',
            'items': era_subject_schema,
        },
        'subscribed': {'type': 'boolean'},
        'is_society_journal': {'type': 'boolean'},
        'use_total': {'type': 'number'},
        'cost_subscription': {'type': 'number'},
        'cost_ill': {'type': 'number'},
        'use_instant': {'type': 'number'},
        'use_instant_percent': {'type': 'number'},
        'use_groups_free_instant': {
            'type': 'object',
            'required': ['oa', 'backfile', 'social_networks'],
            'oa': {'type': 'number'},
            'backfile': {'type': 'number'},
            'social_networks': {'type': 'number'},
        },
        'use_groups_if_subscribed': {
            'type': 'object',
            'required': ['subscription'],
            'subscription': {'type': 'number'},
        },
        'use_groups_if_not_subscribed': {
            'type': 'object',
            'required': ['other_delayed', 'ill'],
            'other_delayed': {'type': 'number'},
            'ill': {'type': 'number'},
        },
        'ncppu': {'type': ['number', 'null', 'string']},
        'ncppu_rank': {'type': ['number', 'null', 'string']},
        'cost': {'type': 'number'},
        'usage': {'type': 'number'},
        'instant_usage_percent': {'type': 'number'},
        'free_instant_usage_percent': {'type': 'number'},
        'subscription_cost': {'type': 'number'},
        'ill_cost': {'type': 'number'},
        'subscription_minus_ill_cost': {'type': 'number'},
        'use_oa_percent': {'type': 'number'},
        'use_backfile_percent': {'type': 'number'},
        'use_subscription_percent': {'type': 'number'},
        'use_ill_percent': {'type': 'number'},
        'use_other_delayed_percent': {'type': 'number'},
        'perpetual_access_years_text': {'type': 'string'},
        'use_asns_percent': {'type': 'number'},
        'use_green_percent': {'type': 'number'},
        'use_hybrid_percent': {'type': 'number'},
        'use_bronze_percent': {'type': 'number'},
        'use_peer_reviewed_percent': {'type': 'number'},
        'oa_embargo_months': {'type': ['number', 'null']},
        'is_hybrid_2019': {'type': 'boolean'},
        'downloads': {'type': 'number'},
        'citations': {'type': 'number'},
        'authorships': {'type': 'number'},
    },

    'additionalProperties': False,
}

journal_to_dict_raw_schema = {
    'type': 'object',

    'required': [
        'meta',
        'table_row',
    ],

    'properties': {
        'meta': {
            'type': 'object',

            'required': [
                'issn_l',
                'title',
                'is_society_journal',
                'oa_embargo_months',
                'subject',
                'era_subjects',
            ],

            'properties': {
                'issn_l': {'type': ['null', 'string']},
                'title': {'type': ['null', 'string']},
                'is_society_journal': {'type': ['null', 'boolean']},
                'oa_embargo_months': {'type': ['null', 'number']},
                'subject': {'type': ['null', 'string']},
                'era_subjects': {
                    'type': 'array',
                    'items': era_subject_schema,
                },
            },

            'additionalProperties': False,
        },

        'table_row': {
            'required': [
                'subscription_cost',
                'ill_cost',
                'use_asns',
                'use_oa',
                'use_backfile',
                'use_subscription',
                'use_ill',
                'use_other_delayed',
                'total_usage',
                'downloads',
                'citations',
                'authorships',
                'has_perpetual_access',
                'perpetual_access_years',
                'baseline_access',
            ],

            'properties': {
                'subscription_cost': {'type': 'number'},
                'ill_cost': {'type': 'number'},
                'use_asns': {'type': 'number'},
                'use_oa': {'type': 'number'},
                'use_backfile': {'type': 'number'},
                'use_subscription': {'type': 'number'},
                'use_ill': {'type': 'number'},
                'use_other_delayed': {'type': 'number'},
                'total_usage': {'type': 'number'},
                'downloads': {'type': 'number'},
                'citations': {'type': 'number'},
                'authorships': {'type': 'number'},
                'has_perpetual_access': {'type': 'boolean'},
                'perpetual_access_years': {
                    'type': 'array',
                    'items': {'type': 'number'}
                },
                'baseline_access': {'type': ['string', 'null']},
            },

            'additionalProperties': False,
        },
    },

    'additionalProperties': False,
}


class TestJournal(ResponseTest):
    def test_dict_details(self):
        # incomplete, only tests journal era subjects

        schema = {
            'type': 'object',
            'required': ['journal'],
            'properties': {
                'journal': {
                    'type': 'object',
                    'required': ['top'],
                    'properties': {
                        'top': {
                            'required': ['era_subjects'],
                            'properties': {
                                'era_subjects': {
                                    'type': 'array',
                                    'items': era_subject_schema,
                                },
                            }
                        }
                    }
                }
            }
        }

        with app.app_context():
            url = dev_request_url('/scenario/crwcRMtB/journal/1087-0792')
            response = self.json_response(url)
            test_name = u'scenario crwcRMtB, journal 1087-0792'
            assert_schema(response, schema, test_name)
