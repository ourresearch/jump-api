from package import Package
from response_test import ResponseTest, dev_request_url, assert_schema
from test_institution import institutions_to_check
from views import app


packages_to_check = [
    p for p in [
        Package.query.filter(Package.institution_id == i).first() for i in institutions_to_check.values()
    ] if p
]


class TestPackage(ResponseTest):
    def test_package_common(self):
        package_schema = {
            'definitions': {
                'apc_authorship': {
                    'type': 'object',

                    'required': [
                        'package_id',
                        'apc',
                        'num_authors_total',
                        'num_authors_from_uni',
                        'issn_l',
                        'doi',
                        'journal_name',
                        'year',
                        'oa_status',
                    ],

                    'properties': {
                        'package_id': {'type': 'string'},
                        'apc': {'type': ['number', 'null']},
                        'num_authors_total': {'type': 'number'},
                        'num_authors_from_uni': {'type': 'number'},
                        'issn_l': {'type': 'string'},
                        'doi': {'type': 'string'},
                        'journal_name': {'type': 'string'},
                        'year': {'type': 'number'},
                        'oa_status': {'type': 'string'},
                    }
                },
                'core_journal': {
                    'required': [
                        'package_id',
                        'issn_l',
                        'baseline_access',
                    ],
                    'properties': {
                        'package_id': {'type': 'string'},
                        'issn_l': {'type': 'string'},
                        'baseline_access': {'type': 'string'},
                    }
                },
                'downloads_dict': {
                    'required': [
                        'issn_l',
                        'issns',
                        'publisher',
                        'subject',
                        'title',
                        'journal_is_oa',
                        'num_papers_2018',
                        'downloads_total',
                        'downloads_0y',
                        'downloads_1y',
                        'downloads_2y',
                        'downloads_3y',
                        'downloads_4y',
                    ],
                    'properties': {
                        'issn_l': {'type': 'string'},
                        'issns': {'type': 'string'},
                        'publisher': {'type': ['string', 'null']},
                        'subject': {'type': ['string', 'null']},
                        'title': {'type': ['string', 'null']},
                        'journal_is_oa': {'type': ['string', 'null']},
                        'num_papers_2018': {'type': ['number', 'null']},
                        'downloads_total': {'type': ['number', 'null']},
                        'downloads_0y': {'type': ['number', 'null']},
                        'downloads_1y': {'type': ['number', 'null']},
                        'downloads_2y': {'type': ['number', 'null']},
                        'downloads_3y': {'type': ['number', 'null']},
                        'downloads_4y': {'type': ['number', 'null']},
                    }
                },
                'oa_journal_row': {
                    'required': [
                        'issn_l',
                        'year_int',
                        'fresh_oa_status',
                        'count',
                    ],
                    'properties': {
                        'issn_l': {'type': ['string', 'null'],},
                        'year_int': {'type': 'number'},
                        'fresh_oa_status': {'type': 'string'},
                        'count': {'type': 'number'},
                    },
                    'additionalProperties': False,
                },
                'oa_type_row': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {
                            'type': 'array',
                            'items': {'$ref': '#/definitions/oa_journal_row'}
                        }
                    },

                    'properties': {
                        'null': {
                            'type': 'array',
                            'items': {'$ref': '#/definitions/oa_journal_row'}
                        }
                    },

                    'additionalProperties': False,
                },
                'oa_recent_journal_row': {
                    'required': [
                        'issn_l',
                        'fresh_oa_status',
                        'count',
                    ],
                    'properties': {
                        'issn_l': {'type': ['string', 'null'], },
                        'fresh_oa_status': {'type': 'string'},
                        'count': {'type': 'number'},
                    },
                    'additionalProperties': False,
                },
                'oa_recent_type_row': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {
                            'type': 'array',
                            'items': {'$ref': '#/definitions/oa_recent_journal_row'}
                        }
                    },

                    'properties': {
                        'null': {
                            'type': 'array',
                            'items': {'$ref': '#/definitions/oa_recent_journal_row'}
                        }
                    },

                    'additionalProperties': False,
                },
            },

            'type': 'object',

            'required': [
                'org_package_ids',
                'apc',
                'core_list',
                'embargo_dict',
                'unpaywall_downloads_dict_raw',
                'oa',
                'oa_recent',
                'social_networks',
                'society',
                'num_papers',
            ],

            'additionalProperties': True,

            'properties': {
                'org_package_ids': {
                    'type': 'array',
                    'items': {'type': 'string'}
                },
                'core_list': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {'$ref': '#/definitions/core_journal'}
                    },

                    'properties': {
                        'null': {'$ref': '#/definitions/core_journal'}
                    },

                    'additionalProperties': False,
                },
                'embargo_dict': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {'type': 'number'}
                    },

                    'properties': {
                        'null': {'type': 'number'}
                    },

                    'additionalProperties': False,
                },
                'unpaywall_downloads_dict_raw': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {'$ref': '#/definitions/downloads_dict'}
                    },

                    'properties': {
                        'null': {'$ref': '#/definitions/downloads_dict'}
                    },

                    'additionalProperties': False,
                },
                'oa': {
                    'type': 'object',

                    'properties': {
                        'no_submitted_no_bronze': {'$ref': '#/definitions/oa_type_row'},
                        'no_submitted_with_bronze': {'$ref': '#/definitions/oa_type_row'},
                        'with_submitted_no_bronze': {'$ref': '#/definitions/oa_type_row'},
                        'with_submitted_with_bronze': {'$ref': '#/definitions/oa_type_row'},
                    },

                    'additionalProperties': False,
                },
                'oa_recent': {
                    'type': 'object',

                    'properties': {
                        'no_submitted_no_bronze': {'$ref': '#/definitions/oa_recent_type_row'},
                        'no_submitted_with_bronze': {'$ref': '#/definitions/oa_recent_type_row'},
                        'with_submitted_no_bronze': {'$ref': '#/definitions/oa_recent_type_row'},
                        'with_submitted_with_bronze': {'$ref': '#/definitions/oa_recent_type_row'},
                    },

                    'additionalProperties': False,
                },
                'social_networks': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {'type': 'number'}
                    },

                    'properties': {
                        'null': {'type': 'number'}
                    },

                    'additionalProperties': False,
                },
                'society': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {
                            'type': 'string',
                            'enum': ['YES', 'NO'],
                        }
                    },

                    'properties': {
                        'null': {
                            'type': 'string',
                            'enum': ['YES', 'NO'],
                        }
                    },

                    'additionalProperties': False,
                },
                'num_papers': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {
                            'type': 'object',

                            'patternProperties': {
                                '^[0-9]{4}$': {
                                    'type': 'number',
                                }
                            },
                        }
                    },

                    'additionalProperties': False,
                },
                'apc': {
                    'type': 'array',
                    'items': {'$ref': '#/definitions/apc_authorship'}
                },
            }
        }

        scenario_schema = {
            'type': 'object',

            'required': [
                'counter_dict',
                'authorship_dict',
                'citation_dict',
            ],

            'properties': {
                'counter_dict': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {'type': 'number'}
                    },

                    'properties': {
                        'null': {'type': 'number'}
                    },

                    'additionalProperties': False,
                },

                'authorship_dict': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {
                            'type': 'object',

                            'patternProperties': {
                                '^[0-9]{4}$': {'type': 'number'}
                            },
                        }
                    },

                    'properties': {
                        'null': {
                            'type': 'object',

                            'patternProperties': {
                                '^[0-9]{4}$': {'type': 'number'}
                            },
                        }
                    },

                    'additionalProperties': False,
                },

                'citation_dict': {
                    'type': 'object',

                    'patternProperties': {
                        '^[0-9]{4}-[0-9xX]{4}$': {
                            'type': 'object',

                            'patternProperties': {
                                '^[0-9]{4}$': {'type': 'number'}
                            },
                        }
                    },

                    'properties': {
                        'null': {
                            'type': 'object',

                            'patternProperties': {
                                '^[0-9]{4}$': {'type': 'number'}
                            },
                        }
                    },

                    'additionalProperties': False,
                },
            },

            'additionalProperties': True,
        }

        with app.app_context():
            for test_package in packages_to_check:
                url = dev_request_url('/live/data/common/{}'.format(test_package.package_id))
                response = self.json_response(url)

                test_name = u'{} {} ({})'.format(
                    test_package.institution.display_name,
                    test_package.package_name,
                    test_package.package_id
                )

                assert_schema(response, package_schema, test_name)

                for org_package_id in response['org_package_ids']:
                    assert_schema(response[org_package_id], scenario_schema, test_name)

    def test_package(self):
        pass

    def test_package_apc(self):
        pass

    def test_package_scenario(self):
        pass

    def test_package_scenario_copy(self):
        pass

    def test_package_counter_diff_no_price(self):
        pass

    def test_package_counter_no_price(self):
        pass
