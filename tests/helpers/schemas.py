from marshmallow import Schema, fields

class ScenarioDetailsJournalsSchema(Schema):
    top = fields.Dict()
    fulfillment = fields.Dict()
    apc = fields.Dict()
    cost = fields.Dict()
    debug = fields.Dict()
    impact = fields.Dict()
    num_papers = fields.Dict()
    num_papers_forecast = fields.Dict()
    oa = fields.Dict()

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

class ScenarioSettings(Schema):
    include_submitted_version = fields.Boolean(required=True)
    include_bronze = fields.Boolean(required=True)
    cost_alacart_increase = fields.Number(required=True)
    weight_citation = fields.Number(required=True)
    cost_content_fee_percent = fields.Number(required=True)
    ill_request_percent_of_delayed = fields.Number(required=True)
    cost_bigdeal_increase = fields.Number(required=True)
    description = fields.Str(required=True)
    notes = fields.Str(required=True)
    cost_ill = fields.Number(required=True)
    include_backfile = fields.Boolean(required=True)
    backfile_contribution = fields.Number(required=True)
    cost_bigdeal = fields.Number(required=True)
    include_social_networks = fields.Boolean(required=True)
    weight_authorship = fields.Number(required=True)

class ScenarioSavedSchema(Schema):
    subrs = fields.List(fields.Str(), required=True)
    configs = fields.Nested(ScenarioSettings, required=True)
    id = fields.Str(required=True)
    name = fields.Str(required=True)
    customSubrs = fields.List(fields.Str(), required=True)
    member_added_subrs = fields.List(fields.Str(), required=True)

class InstitutionSchema(Schema):
    id = fields.Str()
    grid_ids = fields.List(fields.Str())
    ror_ids = fields.List(fields.Str())
    name = fields.Str()
    is_demo = fields.Boolean()
    is_consortium = fields.Boolean()
    is_consortium_member = fields.Boolean()
    user_permissions = fields.List(fields.Dict())
    institutions = fields.List(fields.Dict())
    consortia = fields.List(fields.Dict())
    publishers = fields.List(fields.Dict())
    consortial_proposal_sets = fields.List(fields.Dict())
    is_jisc = fields.Boolean()

# used in test_journal.py
class Top(Schema):
    title = fields.Str()
    cpu = fields.Str()
    use_instant_percent = fields.Number()
    cost_subscription_minus_ill = fields.Str()
    num_papers = fields.Number()
    ill_cost = fields.Str()
    issn_l = fields.Str()
    cost_actual = fields.Str()
    subscription_cost = fields.Str()
    subject = fields.Str()
    subject_top_three = fields.Str()
    subjects_all = fields.List(fields.Str)
    is_society_journal = fields.Boolean()
    subscribed = fields.Boolean()
    era_subjects = fields.List(fields.List(fields.Str))

class FullfillmentUse(Schema):
    total = fields.List(fields.Number)
    oa_plus_social_networks = fields.List(fields.Number)
    backfile = fields.List(fields.Number)
    subscription = fields.List(fields.Number)
    other_delayed = fields.List(fields.Number)
    ill = fields.List(fields.Number)

class Fullfillment(Schema):
    data = fields.List(fields.Dict)
    use_actual_by_year = fields.Nested(FullfillmentUse)
    perpetual_access_years = fields.List(fields.Str)
    has_perpetual_access = fields.Boolean()
    headers = fields.List(fields.Dict)
    downloads_per_paper_by_age = fields.List(fields.Number)
    perpetual_access_years_text = fields.Str()

class JournalDetails(Schema):
    apc = fields.Dict(required=True)
    cost = fields.Dict(required=True)
    debug = fields.Dict(required=True)
    fulfillment = fields.Nested(Fullfillment, required=True)
    impact = fields.Dict(required=True)
    num_papers = fields.Dict(required=True)
    num_papers_forecast = fields.Dict(required=True)
    oa = fields.Dict(required=True)
    top = fields.Nested(Top, required=True)

class JournalSettings(Schema):
    notes = fields.Str()
    cost_ill = fields.Number()
    cost_content_fee_percent = fields.Number()
    cost_alacart_increase = fields.Number()
    weight_authorship = fields.Number()
    cost_bigdeal_increase = fields.Number()
    ill_request_percent_of_delayed = fields.Number()
    include_backfile = fields.Boolean()
    description = fields.Str()
    include_submitted_version = fields.Boolean()
    weight_citation = fields.Number()
    include_bronze = fields.Boolean()
    include_social_networks = fields.Boolean()
    cost_bigdeal = fields.Number()
    backfile_contribution = fields.Number()

class JournalSchema(Schema):
    journal = fields.Nested(JournalDetails, required=True)
    _settings = fields.Nested(JournalSettings, required=True)


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
