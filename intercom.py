import os
import requests
import pandas as pd
from datetime import datetime

consortia_admin_emails = []
zzz = os.getenv("INTERCOM_CONSORTIA_ADMIN_EMAILS")
if zzz:
  consortia_admin_emails = zzz.split(",")

def intercom(emails, domain):
  if not emails:
    return None

  emails_array = [{"field": "email", "operator": "=", "value": w} for w in emails]

  if domain:
    domain_dict = {"field": "email", "operator": "$", "value": domain}
    emails_array.append(domain_dict)

  body = {
   "query":  {
      "operator": "AND",
      "value": [
        {
          "field": "segment_id",
          "operator": "=",
          "value": "622995922effdb52e71bb464" # segment: "All users-Not-Portland"
        },
        {
          "operator": "OR",
          "value": emails_array
        }
      ]
    }
  }

  intercom_base = "https://api.intercom.io"
  key = os.getenv("INTERCOM_API_KEY")
  auth = {"Authorization": "Bearer " + key}
  res = requests.post(intercom_base + "/contacts/search", json=body, headers=auth)
  last_seen_at = None
  if res.ok:
    data = res.json()
    # remove consortia admins
    data = list(filter(lambda x: x['email'] not in consortia_admin_emails, data['data']))
    # filter to emails searched or domain for organization
    data = list(filter(lambda x: x['email'] in emails or domain in x['email'], data))
    times = [datetime.fromtimestamp(w['last_seen_at']) for w in data if w['last_seen_at'] is not None]
    if len(times) > 1:
      last_seen_at = max(times)
    elif len(times) == 0:
      last_seen_at = None
    else:
      last_seen_at = times[0]

  if isinstance(last_seen_at, pd.Timestamp) or isinstance(last_seen_at, datetime):
    last_seen_at = last_seen_at.strftime("%Y-%m-%d")

  return last_seen_at
