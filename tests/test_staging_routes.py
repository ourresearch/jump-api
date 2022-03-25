import pytest
import requests
import json
import os
import re
# import heroku3

from .helpers.http import url_base, skip_if_down, fetch_jwt
from schemas import user_schema, user_permissions_schema
from .helpers.response_test import assert_schema
from views import app

# def staging_conn():
#     # create client
#     heroku_conn = heroku3.from_key(os.environ['HEROKU_KEY_OURRESEARCH'])
#     # for staging
#     app = heroku_conn.apps()[os.environ['HEROKU_UNSUB_APPNAME_STAGING']]
#     # get process formation
#     proc = app.process_formation()['web']
#     return proc

# def staging_on(con):
#     con.scale(1)
#     return print("Unsub staging on")

# def staging_off(con):
#     con.scale(0)
#     return print("Unsub staging off")

# con = staging_conn()
# staging_on(con)
# jwt = fetch_jwt()

def test_staging_api_root():
    skip_if_down()
    x = requests.get(url_base)
    assert x.status_code == 200
    assert x.json() == {"msg": "Don't panic", "version": "0.0.1"}


def test_staging_api_login():
    skip_if_down()
    res = requests.post(
        url_base + "/user/login",
        json={
            "password": os.environ["UNSUB_USER1_PWD"],
            "email": os.environ["UNSUB_USER1_EMAIL"],
        },
    )
    tok = res.json()["access_token"]
    assert res.status_code == 200
    assert isinstance(tok, str)

def test_staging_api_user_me(fetch_jwt):
    res = requests.get(
        url_base + "/user/me", 
        headers={"Authorization": "Bearer " + fetch_jwt(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])}
    )
    assert res.status_code == 200
    assert isinstance(res.json(), dict)

    # with app.app_context():
    # Is the user data of the right shape and types?
    assert_schema(res.json(), user_schema, "/user/me")

def test_staging_api_user_permissions(fetch_jwt):
    res = requests.get(
        url_base + "/user-permissions",
        headers={"Authorization": "Bearer " + fetch_jwt(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])},
        json={
            "user_id": os.environ["UNSUB_USER1_ID"],
            "institution_id": os.environ["UNSUB_USER1_INSTITUTION_ID"],
            "email": os.environ["UNSUB_USER1_EMAIL"],
        },
    )
    assert res.status_code == 200
    assert isinstance(res.json(), dict)

    # with app.app_context():
    # Is the user-permissions data of the right shape and types?
    assert_schema(res.json(), user_permissions_schema, "/user-permissions")

def test_staging_api_account(fetch_jwt):
    res = requests.get(
        url_base + "/account", 
        headers={"Authorization": "Bearer " + fetch_jwt(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])},
    )
    assert res.status_code == 404
    assert re.match("Removed. Use /user/me", res.json()["message"])

def test_staging_download_price_custom_file(fetch_jwt):
    package_id = 'package-jisctfnth'
    res = requests.get(
        url_base + f"/publisher/{package_id}/price",
        headers={"Authorization": "Bearer " + fetch_jwt(os.environ["UNSUB_JISC_PWD"], os.environ["UNSUB_JISC_EMAIL"])},
    )
    assert res.status_code == 200
    if res.ok:
        body = res.json()
        assert isinstance(body, dict)
        assert isinstance(body['rows'][0], dict)
        assert isinstance(body['rows'][0]['issn_l'], str)

def test_staging_download_pta_file(fetch_jwt):
    package_id = 'package-jisctfnth'
    res = requests.get(
        url_base + f"/publisher/{package_id}/perpetual-access",
        headers={"Authorization": "Bearer " + fetch_jwt(os.environ["UNSUB_JISC_PWD"], os.environ["UNSUB_JISC_EMAIL"])},
    )
    assert res.status_code == 200
    if res.ok:
        body = res.json()
        assert isinstance(body, dict)
        assert isinstance(body['rows'][0], dict)
        assert isinstance(body['rows'][0]['issn_l'], str)

# see https://github.com/ourresearch/unsub-private/issues/23
def test_staging_zoom_scenario_issnl(fetch_jwt):
    # consortium_scenario_id = 'e5tqtgQQ' # team+consortiumtest@ourresearch.org
    scenario_id = 'Jad3Cc6a' # scott+test@ourresearch.org, package-erBCoZrj6V9Q
    good_issn_l = '1347-4367'
    bad_issn_l = '999X-X999'
    jwt = fetch_jwt(os.environ["UNSUB_USER1_PWD"], os.environ["UNSUB_USER1_EMAIL"])
    
    good_res = requests.get(
        url_base + f"/scenario/{scenario_id}/journal/{good_issn_l}",
        headers={"Authorization": "Bearer " + jwt},
    )
    bad_res = requests.get(
        url_base + f"/scenario/{scenario_id}/journal/{bad_issn_l}",
        headers={"Authorization": "Bearer " + jwt},
    )
    
    assert good_res.status_code == 200
    assert bad_res.status_code == 404
    
    if good_res.ok:
        body = good_res.json()
        assert list(body.keys()) == ['_settings', 'journal']

    if not bad_res.ok:
        body = bad_res.json()
        assert body['error']
        assert "not found in scenario" in body['message']

# staging_off(con)
