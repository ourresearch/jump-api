import pytest
import psycopg2.extras
import re
from consortium import Consortium

scenario_id = 'EL9sZJQP'
scenario_id_with_email = 'scenario-F3awPQcDiG7C'
cons = Consortium(scenario_id=scenario_id_with_email)

# Make sure some of the methods that use psycopg2 sql bind variables are working as expected
# all with prefix 'test_bindvars_'
def test_bindvars_get_latest_member_institutions_raw():
    from consortium import get_latest_member_institutions_raw
    res = get_latest_member_institutions_raw(scenario_id)
    assert isinstance(res, list)
    for x in res:
        assert isinstance(x, str)
    assert re.match(r'package-.+', res[0])

def test_bindvars_consortium_get_computed_data():
    from consortium import consortium_get_computed_data
    res = consortium_get_computed_data(scenario_id)
    assert isinstance(res, list)
    for x in res:
        assert isinstance(x, dict)
    assert isinstance(res[0]['usage'], float)

def test_bindvars_consortium_get_issns():
    from consortium import consortium_get_issns
    res = consortium_get_issns(scenario_id)
    assert isinstance(res, list)
    for x in res:
        assert re.match(r'\d+-\d+', x)

def test_bindvars_all_member_package_ids():
    res = cons.all_member_package_ids
    assert isinstance(res, list)
    for x in res:
        assert isinstance(x, str)
    assert re.match(r'package-.+', res[0])

def test_bindvars_to_dict_journal_zoom():
    res = cons.to_dict_journal_zoom('0164-0704')
    assert isinstance(res, list)
    assert len(res) > 0
    assert isinstance(res[0], psycopg2.extras.RealDictRow)
    assert list(res[0].keys()) == ['institution_id', 'institution_name', 'package_id', 'usage', 'cpu']

def test_bindvars_get_perpetual_access_from_cache():
    res = cons.to_dict_institutions()
    assert isinstance(res, list)
    assert len(res) > 0
    assert isinstance(res[0], psycopg2.extras.RealDictRow)
    assert list(res[0].keys()) == ['institution_id', 'institution_short_name', 'institution_name', 'package_id', 'usage', 'num_journals', 'tags', 'included']
