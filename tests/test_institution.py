import pytest
import requests
import os
import json
from collections import OrderedDict
from marshmallow import Schema, fields, ValidationError 
from flask_jwt_extended import verify_jwt_in_request
from views import app
from package import Package # required so that self.packages (in Institution) is populated
from institution import Institution
from util import jsonify_fast_no_sort
from .helpers.http import url_base, skip_if_down, fetch_jwt, fetch_jwt4testing
from .helpers.schemas import InstitutionSchema

institutions_to_check = {
    'consortia-test': 'institution-WzH2RdcHUPoR',
    'scott': "institution-tetA3UnAr3dV"
}

# jwt = fetch_jwt4testing(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])
# jwt_secret_key = os.getenv('JWT_SECRET_KEY')

def test_institutions_api(fetch_jwt):
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

def inst_to_dict(x):
    return OrderedDict([
        ("id", x.id),
        ("grid_ids", [g.grid_id for g in x.grid_ids]),
        ("ror_ids", [r.ror_id for r in x.ror_ids]),
        ("name", x.display_name),
        ("is_demo", x.is_demo_institution),
        ("is_consortium", x.is_consortium),
        ("is_consortium_member", x.is_consortium_member),
        ("user_permissions", [{}]),
        ("institutions", [{}]),
        ("consortia", []),
        ("publishers", [p.to_dict_minimal() for p in x.packages_sorted]),
        ("consortial_proposal_sets", [p.to_dict_minimal_feedback_set() for p in x.packages_sorted if p.is_feedback_package]),
        ("is_jisc", x.is_jisc)
    ])

# institution_id='institution-JnorxoyU4D8g'
def test_institutions_app():
    for institution_name, institution_id in institutions_to_check.items():
        my_institution = Institution.query.get(institution_id)
        my_dict = inst_to_dict(my_institution)
        
        out = InstitutionSchema().load(my_dict)
        
        with pytest.raises(ValidationError) as err:
            InstitutionSchema().load({"id": 5})
            assert "Not a valid string" in str(err.value)

