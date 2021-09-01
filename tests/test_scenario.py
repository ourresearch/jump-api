import pytest
import requests
from marshmallow import Schema, fields, ValidationError 
from .helpers.http import url_base, skip_if_down, fetch_jwt
from schemas import ScenarioMetaSchema, ScenarioSavedSchema

def test_scenario_journals(fetch_jwt):
	res = requests.get(
		url_base + "/scenario/Jrofb6CY/journals",
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
