import json
import unittest
import pytest
from .helpers.response_test import dev_request_url, assert_schema
from views import app
from marshmallow import Schema, fields, ValidationError 

institutions_to_check = {
    # 'suny': 'institution-2h4YWhLmGQSy',
    # 'suny_poly': 'institution-iRFuoxgDGH5z',
    # 'suny_a': 'institution-ZPY8mUmpK2gM',
    # 'uva': 'institution-xFFDfqtaBXik',
    'scott': "institution-tetA3UnAr3dV"
}

class Response(object):
    def __init__(self):
        self.client = app.test_client()
        self.client.testing = True

    def json_response(self, url):
        return json.loads(self.client.get(url).get_data(as_text=True))

def test_institutions():
    # schema = {
    #     'definitions': {
    #         'user_permissions': {
    #             'type': 'object',

    #             'required': [
    #                 'username',
    #                 'institution_id',
    #                 'is_authenticated_user',
    #                 'permissions',
    #                 'user_name',
    #                 'user_id',
    #                 'institution_name',
    #                 'user_email',
    #             ],

    #             'properties': {
    #                 'username': {'type': ['string', 'null']},
    #                 'institution_id': {'type': 'string'},
    #                 'is_authenticated_user': {'type': 'boolean'},
    #                 'permissions': {
    #                     'type': 'array',
    #                     'items': {'type': 'string'}
    #                 },
    #                 'user_name': {'type': ['string', 'null']},
    #                 'user_id': {'type': 'string'},
    #                 'institution_name': {'type': 'string'},
    #                 'user_email': {'type': ['string', 'null']},
    #             }
    #         },

    #         'publisher': {
    #             'type': 'object',

    #             'required': [
    #                 'name',
    #                 'id',
    #                 'is_deleted',
    #             ],

    #             'properties': {
    #                 'id': {'type': 'string'},
    #                 'name': {'type': ['string', 'null']},
    #                 'is_deleted': {'type': 'boolean'},
    #             },
    #         }
    #     },

    #     'type': 'object',

    #     'required': [
    #         'id',
    #         'ror_ids',
    #         'name',
    #         'user_permissions',
    #         'publishers',
    #         'grid_ids',
    #         'is_demo',
    #     ],

    #     'properties': {
    #         'id': {'type': 'string'},
    #         'ror_ids': {
    #             'type': 'array',
    #             'items': {'type': 'string'}
    #         },
    #         'name': {'type': 'string'},
    #         'user_permissions': {
    #             'type': 'array',
    #             'items': {'$ref': '#/definitions/user_permissions'}
    #         },
    #         'publishers': {
    #             'type': 'array',
    #             'items': {'$ref': '#/definitions/publisher'}
    #         },
    #         'grid_ids': {
    #             'type': 'array',
    #             'items': {'type': 'string'}
    #         },
    #         'is_demo': {'type': 'boolean'},
    #     }
    # }
    schema = {
        'type': 'object',
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
    class InstitutionSchema(Schema):
        id = fields.Str()
        grid_ids = fields.List(fields.Str())
        ror_ids = fields.List(fields.Str())
        name = fields.Str()
        is_demo = fields.Boolean()
        is_consortium = fields.Boolean()
        is_consortium_member = fields.Boolean()
        user_permissions = fields.List(fields.Dict())

    with app.app_context():
        for institution_name, institution_id in institutions_to_check.items():
            url = dev_request_url('/institution/{}'.format(institution_id))
            response = Response().json_response(url)
            # response = testget(url)
            assert_schema(response, schema, institution_name)
