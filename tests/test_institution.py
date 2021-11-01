import pytest
import requests
import os
from marshmallow import Schema, fields, ValidationError 
from .helpers.http import url_base, skip_if_down, fetch_jwt
from .helpers.schemas import InstitutionSchema

institutions_to_check = {
    'consortia-test': 'institution-WzH2RdcHUPoR',
    'scott': "institution-tetA3UnAr3dV"
}

def test_institutions(fetch_jwt):
    for institution_name, institution_id in institutions_to_check.items():
        res = requests.get(
            url_base + f"/institution/{institution_id}",
            headers={"Authorization": "Bearer " + fetch_jwt(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])},
        )
        assert res.status_code == 200
        
        out = InstitutionSchema().load(res.json())
        
        with pytest.raises(ValidationError) as err:
            InstitutionSchema().load({"id": 5})
            assert "Not a valid string" in str(err.value)
