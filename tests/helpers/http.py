import pytest
import os
import requests

url_base = os.environ["UNSUB_TEST_URL_STAGING"]

def skip_if_down():
    if not requests.get(url_base):
        pytest.skip("Unsub Staging API is down")

@pytest.fixture
def fetch_jwt():
    skip_if_down()
    res = requests.post(
        url_base + "/user/login",
        json={
            "password": os.environ["UNSUB_USER1_PWD"],
            "email": os.environ["UNSUB_USER1_EMAIL"],
        },
    )
    return res.json()["access_token"]

