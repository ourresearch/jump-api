import pytest
import requests
import json
import os
import string
import random

from tests.helpers.http import url_base, skip_if_down, fetch_jwt
from app import app
from saved_scenario import SavedScenario

def random_string(with_single_quote=False):
    letters = list(string.ascii_lowercase)
    word = ''
    for _ in range(5):
        random_index = random.randrange(len(letters))
        word += letters[random_index]
    if with_single_quote:
        return word + "'" + word
    return word

package_id = 'package-NZe2awMnex6K'
scenario_id = 'CUc7kqN8'
new_scenario_name = random_string(with_single_quote=True)
# new_scenario_name = "lulz'bar"

# tests route: POST /scenario/<scenario_id> (Use case: Rename a scenario)
def test_updating_scenario_name(fetch_jwt):
    jwt = fetch_jwt(os.environ["UNSUB_USER2_PWD"], os.environ["UNSUB_USER2_EMAIL"])
    # jwt = fetch_jwt()
    my_saved_scenario = SavedScenario.query.get(scenario_id)
    payload = my_saved_scenario.to_dict_saved_from_db()
    payload['name'] = new_scenario_name

    with app.test_client() as c:
        x = c.post(f"/scenario/{scenario_id}",
            data = json.dumps(payload),
            content_type = "application/json",
            headers={"Authorization": "Bearer " + jwt}
        )

    assert x.json == {'status': 'success'}

    # make sure the change happened
    with app.test_client() as c:
        res = c.get('/package/{}'.format(my_saved_scenario.package_id),
            headers={"Authorization": "Bearer " + jwt}
        )

    assert res.json['scenarios'][0]['name'] == new_scenario_name

    # # http request against staging
    # res = requests.post(
    #     url_base + f"/scenario/{scenario_id}",
    #     data = json.dumps(payload),
    #     headers={"Authorization": "Bearer " + jwt, 'Content-type': 'application/json'}
    # )
    # assert res.status_code == 200
    # assert isinstance(res.json(), dict)
