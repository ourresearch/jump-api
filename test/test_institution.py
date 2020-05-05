from response_test import ResponseTest, dev_request_url, assert_schema

#  heroku local:run nosetests test/test_institution.py


institutions_to_check = {
    'suny': 'institution-2h4YWhLmGQSy',
    'suny_poly': 'institution-iRFuoxgDGH5z',
    'suny_a': 'institution-ZPY8mUmpK2gM',
    'uva': 'institution-xFFDfqtaBXik',
}


class TestInstitution(ResponseTest):
    def test_institutions(self):
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

        for institution_name, institution_id in institutions_to_check.items():
            url = dev_request_url('/institution/{}'.format(institution_id))
            response = self.json_response(url)
            assert_schema(response, schema, institution_name)


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