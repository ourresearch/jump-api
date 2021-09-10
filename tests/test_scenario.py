import pytest
import requests
from marshmallow import Schema, fields, ValidationError 
from .helpers.http import url_base, skip_if_down, fetch_jwt
from .helpers.schemas import ScenarioMetaSchema, ScenarioSavedSchema, ScenarioDetailsJournalsSchema

scenario_id = "Jrofb6CY"

def test_scenario_journals(fetch_jwt):
	res = requests.get(
		url_base + f"/scenario/{scenario_id}/journals",
		headers={"Authorization": "Bearer " + fetch_jwt},
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
	assert out["meta"]["institution_name"] == 'University of Scott'

	with pytest.raises(ValidationError):
		ScenarioJournalsSchema().load({"meta": 5})

# FIXME: DELETE method only, test with fixture
# def test_scenario(fetch_jwt):
#     pass

# NOTE: from logs appears this route is never or rarely used
def test_scenario_details(fetch_jwt):
	res = requests.get(
		url_base + f"/scenario/{scenario_id}/details",
		headers={"Authorization": "Bearer " + fetch_jwt},
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

def test_scenario_subscriptions(fetch_jwt):
	pass

def test_scenario_export(fetch_jwt):
	pass
