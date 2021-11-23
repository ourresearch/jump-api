import pytest
import requests
import os
from io import StringIO
import pandas as pd
from marshmallow import Schema, fields, ValidationError
from .helpers.http import url_base, skip_if_down, fetch_jwt
from .helpers.schemas import (
    ScenarioMetaSchema,
    ScenarioSavedSchema,
    ScenarioDetailsJournalsSchema,
)
import psycopg2.extras
import re

# IDs in one of Scott's test accounts - NOT related to a consortium
scenario_id = "Jrofb6CY"
package_id = "package-iQF8sFiRY99t"

# a scenario in one of Scott's test accounts - IS related to a consortium
consortium_scenario_id = "e5tqtgQQ"


def test_scenario_journals(fetch_jwt):
    res = requests.get(
        url_base + f"/scenario/{scenario_id}/journals",
        headers={"Authorization": "Bearer " + fetch_jwt(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])},
    )
    assert res.status_code == 200
    assert isinstance(res.json(), dict)

    class ScenarioJournalsSchema(Schema):
        meta = fields.Nested(ScenarioMetaSchema)
        _debug = fields.Dict()
        saved = fields.Nested(ScenarioSavedSchema)
        consortial_proposal_dates = fields.Dict()
        journals = fields.List(fields.Dict())
        is_locked_pending_update = fields.Boolean()
        update_notification_email = fields.Str(allow_none=True)
        update_percent_complete = fields.Number(allow_none=True)
        warnings = fields.List(fields.Dict)
        _timing = fields.List(fields.Str)

    out = ScenarioJournalsSchema().load(res.json())
    assert isinstance(out, dict)
    assert out["meta"]["institution_name"] == "University of Scott"

    with pytest.raises(ValidationError):
        ScenarioJournalsSchema().load({"meta": 5})


# FIXME: DELETE method only, test with fixture
# def test_scenario(fetch_jwt):
#     pass

# NOTE: from logs appears this route is never or rarely used, so doing minimal testing
def test_scenario_details(fetch_jwt):
    res = requests.get(
        url_base + f"/scenario/{scenario_id}/details",
        headers={"Authorization": "Bearer " + fetch_jwt(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])},
    )
    data = res.json()
    assert res.status_code == 200
    assert isinstance(data, dict)

    class ScenarioDetailsSchema(Schema):
        journals = fields.List(fields.Nested(ScenarioDetailsJournalsSchema))
        _timing = fields.List(fields.Str)
        _settings = fields.Dict()
        _summary = fields.Dict()

    out = ScenarioDetailsSchema().load(data)
    assert out["_settings"]["ill_request_percent_of_delayed"] == 5

    with pytest.raises(ValidationError):
        ScenarioDetailsSchema().load({"meta": 5})
        ScenarioDetailsSchema().load({"_stuff": 5})


# FIXME: this route depends on current state, so need to do account for that somehow
#   If run without doing so, it ends up inserting a `null` into scenario_json field
#   in jump_scenario_details_paid table
# def test_scenario_subscriptions(fetch_jwt):
# 	res = requests.post(
# 		url_base + f"/scenario/{scenario_id}/subscriptions",
# 		headers={"Authorization": "Bearer " + fetch_jwt},
# 	)
# 	data = res.json()
# 	assert res.status_code == 200
# 	assert isinstance(data, dict)
# 	assert list(data.keys()) == ['status','_timing',]
# 	assert data['status'] == 'success'
# 	assert any(['post_subscription_guts' in x for x in data['_timing']])


def test_scenario_export(fetch_jwt):
    res = requests.get(
        url_base + f"/scenario/{scenario_id}/export",
        headers={"Authorization": "Bearer " + fetch_jwt(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])},
    )
    data = res.text
    assert res.status_code == 200
    assert "text/text" in res.headers["Content-Type"]
    assert isinstance(data, str)

    f = StringIO(data)
    x = pd.read_csv(f)
    assert isinstance(x, pd.core.frame.DataFrame)
    assert isinstance(x.iloc[0], pd.core.series.Series)


def test_scenario_export_csv(fetch_jwt):
    res = requests.get(
        url_base + f"/scenario/{scenario_id}/export.csv",
        headers={"Authorization": "Bearer " + fetch_jwt(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])},
    )
    data = res.text
    assert res.status_code == 200
    assert "text/csv" in res.headers["Content-Type"]
    assert isinstance(data, str)

    f = StringIO(data)
    x = pd.read_csv(f)
    assert isinstance(x, pd.core.frame.DataFrame)
    assert isinstance(x.iloc[0], pd.core.series.Series)


# FIXME: appears to not be used at all, skipping tests
# def test_scenario_summary(fetch_jwt):
#     pass


def test_scenario_member_institutions(fetch_jwt):
    jwt = fetch_jwt(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])
    # scenario id that's not part of a consortium
    res = requests.get(
        url_base + f"/scenario/{scenario_id}/member-institutions",
        headers={"Authorization": "Bearer " + jwt},
    )
    assert res.status_code == 404
    assert res.json()["message"] == "not a consortium scenario_id"

    # scenario id that is part of a consortium
    res = requests.get(
        url_base + f"/scenario/{consortium_scenario_id}/member-institutions",
        headers={"Authorization": "Bearer " + jwt},
    )
    data = res.json()
    assert res.status_code == 200
    assert isinstance(data, dict)
    assert isinstance(data["institutions"], list)
    assert isinstance(data["institutions"][0], dict)
    assert isinstance(data["institutions"][0]["institution_name"], str)


# Make sure some of the methods that use psycopg2 sql bind variables are working as expected
# all with prefix 'test_scenario_bindvars_'
def test_scenario_bindvars_get_consortium_package_ids():
    from scenario import get_consortium_package_ids
    res = get_consortium_package_ids('uwdhDaJ2')
    assert isinstance(res, list)
    assert len(res) > 0

def test_scenario_bindvars_get_counter_journals_by_report_name_from_db():
    from scenario import get_counter_journals_by_report_name_from_db
    res = get_counter_journals_by_report_name_from_db(package_id)
    assert isinstance(res, list)
    assert len(res) > 0

def test_scenario_bindvars_get_counter_totals_from_db():
    from scenario import get_counter_totals_from_db
    res = get_counter_totals_from_db(package_id)
    assert isinstance(res, dict)
    assert len(res) > 0

def test_scenario_bindvars_get_package_specific_scenario_data_from_db():
    from scenario import get_package_specific_scenario_data_from_db
    res = get_package_specific_scenario_data_from_db(package_id)
    assert isinstance(res, dict)
    assert len(res) > 0
    assert list(res.keys()) == ['timing', 'counter_dict', 'citation_dict', 'authorship_dict']

def test_scenario_bindvars_get_apc_data_from_db():
    from scenario import get_apc_data_from_db
    res = get_apc_data_from_db(package_id)
    assert isinstance(res, list)
    assert len(res) > 0
    assert isinstance(res[0], psycopg2.extras.RealDictRow)
    assert list(res[0].keys()) == ['package_id', 'doi', 'num_authors_total', 'num_authors_from_uni', 'journal_name', 'issn_l', 'year', 'oa_status', 'apc', 'publisher']

def test_scenario_bindvars_get_perpetual_access_from_cache():
    from scenario import get_perpetual_access_from_cache
    res = get_perpetual_access_from_cache(package_id)
    assert isinstance(res, dict)
    assert len(res) > 0
    for x in list(res.keys()):
        assert re.match(r'\d+-\d+', x)

def test_scenario_bindvars_get_core_list_from_db():
    from scenario import get_core_list_from_db
    res = get_core_list_from_db("51e8103d")
    assert isinstance(res, dict)
    assert len(res) > 0
    out = list(res.items())
    assert isinstance(out[0][0], str)
    assert isinstance(out[0][1], psycopg2.extras.DictRow)
