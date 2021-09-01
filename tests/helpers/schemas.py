from marshmallow import Schema, fields

# FIXME: double check this, I'm not actually sure these fields are required
user_schema = {
    "type": "object",
    "required": [
        "id",
        "name",
        "email",
        "username",
        "is_demo",
        "is_password_set",
        "user_permissions",
        "institutions",
        "consortia",
    ],
    "properties": {
        "id": {"type": "string"},
        "name": {"type": "string"},
        "email": {"type": "string"},
        "username": {"type": ["string", "null"]},
        "is_demo": {"type": ["boolean", "null"]},
        "is_password_set": {"type": "boolean"},
        "user_permissions": {"type": "array"},
        "institutions": {"type": "array"},
        "consortia": {"type": "array"},
    },
}

# FIXME: double check this, I'm not actually sure these fields are required
user_permissions_schema = {
    "type": "object",
    "required": [
        "institution_id",
        "user_id",
        "user_email",
        "username",
        "permissions",
        "institution_name",
        "is_consortium",
        "user_name",
        "is_authenticated_user",
        "is_demo_institution",
    ],
    "properties": {
        "institution_id": {"type": "string"},
        "user_id": {"type": "string"},
        "user_email": {"type": "string"},
        "username": {"type": ["string", "null"]},
        "permissions": {"type": "array"},
        "institution_name": {"type": "string"},
        "is_consortium": {"type": "boolean"},
        "user_name": {"type": "string"},
        "is_authenticated_user": {"type": "boolean"},
        "is_demo_institution": {"type": "boolean"},
    },
}

# from old tests, Scott didn't make these
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

# scenario_meta_schema = {
#     'type': 'object',

#     'required': [
#         'scenario_id',
#         'scenario_name',
#         'publisher_id',
#         'publisher_name',
#         'institution_id',
#         'institution_name',
#         'scenario_created',
#         'is_base_scenario',
#     ],

#     'properties': {
#         'scenario_id': {'type': 'string'},
#         'scenario_name': {'type': ['string', 'null']},
#         'publisher_id': {'type': 'string'},
#         'publisher_name': {'type': 'string'},
#         'institution_id': {'type': 'string'},
#         'institution_name': {'type': 'string'},
#         'scenario_created': {'type': 'string'},
#         'is_base_scenario': {'type': 'boolean'},
#     },

#     'additionalProperties': False,
# }

class ScenarioMetaSchema(Schema):
    scenario_id = fields.Str(required=True)
    scenario_name = fields.Str(required=True, allow_none=True)
    scenario_description = fields.Str(allow_none=True)
    scenario_notes = fields.Str(allow_none=True)
    publisher_id = fields.Str(required=True)
    publisher_name = fields.Str(required=True)
    institution_id = fields.Str(required=True)
    institution_name = fields.Str(required=True)
    cost_bigdeal = fields.Number(required=True)
    cost_bigdeal_increase = fields.Number(required=True)
    scenario_created = fields.Str(required=True)
    is_base_scenario = fields.Boolean(required=True)
    is_consortial_proposal = fields.Boolean(required=True)

# scenario_settings = {
#     'type': 'object',

#     'required': [
#         'cost_bigdeal',
#         'include_social_networks',
#         'include_bronze',
#         'include_submitted_version',
#         'cost_ill',
#         'cost_bigdeal_increase',
#         'cost_content_fee_percent',
#         'ill_request_percent_of_delayed',
#         'cost_alacart_increase',
#         'backfile_contribution',
#         'weight_authorship',
#         'weight_citation',
#         'include_backfile',
#     ],

#     'properties': {
#         'cost_bigdeal': {'type': 'number'},
#         'include_social_networks': {'type': 'boolean'},
#         'include_bronze': {'type': 'boolean'},
#         'include_submitted_version': {'type': 'boolean'},
#         'cost_ill': {'type': 'number'},
#         'cost_bigdeal_increase': {'type': 'number'},
#         'cost_content_fee_percent': {'type': 'number'},
#         'ill_request_percent_of_delayed': {'type': 'number'},
#         'cost_alacart_increase': {'type': 'number'},
#         'backfile_contribution': {'type': 'number'},
#         'weight_authorship': {'type': 'number'},
#         'weight_citation': {'type': 'number'},
#         'include_backfile': {'type': 'boolean'},
#         'package': {'type': 'string'},
#     },

#     'additionalProperties': False,
# }

class ScenarioSettings(Schema):
    include_submitted_version = fields.Boolean()
    include_bronze = fields.Boolean()
    cost_alacart_increase = fields.Str()
    weight_citation = fields.Str()
    cost_content_fee_percent = fields.Str()
    ill_request_percent_of_delayed = fields.Str()
    cost_bigdeal_increase = fields.Number()
    description = fields.Str()
    notes = fields.Str()
    cost_ill = fields.Str()
    include_backfile = fields.Boolean()
    backfile_contribution = fields.Number()
    cost_bigdeal = fields.Number()
    include_social_networks = fields.Boolean()
    weight_authorship = fields.Number()

# scenario_saved_schema = {
#     'type': 'object',

#     'required': [
#         'subrs',
#         'customSubrs',
#         'configs',
#         'name',
#         'id',
#     ],

#     'properties': {
#         'subrs': {'type': 'array', 'items': {'type': 'string'}},
#         'customSubrs': {'type': 'array', 'items': {'type': 'string'}},
#         'configs': scenario_settings,
#         'name': {'type': 'string'},
#         'id': {'type': 'string'},
#     },

#     'additionalProperties': False,
# }

class ScenarioSavedSchema(Schema):
    subrs = fields.List(fields.Str(), required=True)
    configs = fields.Nested(ScenarioSettings, required=True)
    id = fields.Str(required=True)
    name = fields.Str(required=True)
    customSubrs = fields.List(fields.Str(), required=True)
    member_added_subrs = fields.List(fields.Str(), required=True)
