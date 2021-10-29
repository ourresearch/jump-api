import pytest
import requests
import json
import os

from tests.helpers.http import url_base, skip_if_down, fetch_jwt
from views import app

scenario_id = 'CUc7kqN8'
new_scenario_name = "foo'bar"

def test_updating_scenario_name(fetch_jwt):
    skip_if_down()
    x = requests.post(url_base + '/scenario/{}'.format(scenario_id),
        json={
            # "password": os.environ["UNSUB_USER1_PWD"],
            # "email": os.environ["UNSUB_USER1_EMAIL"],
            "name": new_scenario_name
        },
        headers={"Authorization": "Bearer " + fetch_jwt()},
    )

    assert x.status_code == 200
    assert x.json() == {"msg": "Don't panic", "version": "0.0.1"}

