import pytest
import requests
from marshmallow import Schema, fields, ValidationError
from .helpers.http import url_base, skip_if_down, fetch_jwt
from .helpers.schemas import Top, FullfillmentUse, Fullfillment, JournalDetails, JournalSettings, JournalSchema

def test_journal(fetch_jwt):
    res = requests.get(
        url_base + "/scenario/Jrofb6CY/journal/0892-1997",
        headers={"Authorization": "Bearer " + fetch_jwt},
    )
    assert res.status_code == 200
    assert isinstance(res.json(), dict)

    out = JournalSchema().load(res.json())
    assert isinstance(out, dict)

    with pytest.raises(ValidationError) as err:
        JournalSchema().load({"journal": {"top": []}, "_settings": {"notes": 5}})
        assert "Not a valid string" in str(err.value)
        assert "Missing data for required field" in str(err.value)
