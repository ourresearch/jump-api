import pytest
import os
import requests

url_base = os.environ["UNSUB_TEST_URL_STAGING"]

def skip_if_down():
    if not requests.get(url_base):
        pytest.skip("Unsub Staging API is down")

@pytest.fixture
def fetch_jwt():
    def _fetch_jwt(pwd, email):
        skip_if_down()
        res = requests.post(
            url_base + "/user/login",
            json={
                "password": pwd,
                "email": email
            },
        )
        return res.json()["access_token"]
    return _fetch_jwt

def fetch_jwt4testing(pwd, email):
    skip_if_down()
    res = requests.post(
        url_base + "/user/login",
        json={
            "password": pwd,
            "email": email
        },
    )
    return res.json()["access_token"]
