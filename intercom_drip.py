import os
import ast
import json
from datetime import datetime
from datetime import date

from kids.cache import cache
import click
import requests
import pandas as pd
import gspread


# intercom API details
intercom_base = "https://api.intercom.io"
key = os.getenv("INTERCOM_API_KEY")
auth = {"Authorization": "Bearer " + key}

# google sheets credentials
json_cred = os.environ.get('GOOGLE_SHEETS_JSON')
file_cred = 'google_sheets_creds.json'
with open(file_cred, 'w') as f:
    json.dump(json.loads(json_cred), f, ensure_ascii=False, indent=4)
gc = gspread.service_account(file_cred)

@cache
def inst_level_data_prep():
  sheet_pkg = gc.open('unsub-institution-level')
  worksheet = sheet_pkg.get_worksheet(0)
  df = pd.DataFrame(worksheet.get_all_records())
  df = df[df['intercom_last_seen_email'].str.len() > 0]
  df['new_users'] = ['False' if not w else w for w in df['new_users']]
  df['new_users'] = [ast.literal_eval(w.title()) for w in df['new_users']]
  df['not_using'] = ['False' if not w else w for w in df['not_using']]
  df['not_using'] = [ast.literal_eval(w.title()) for w in df['not_using']]
  # remove any in a consortium where data is set up for them
  ## jisc, irel, crkn 
  df = df[~df['consortium_account'].str.contains("JISC|IreL|CRKN")]
  df = df[df['institution_id'].str.contains("jisc", na = False) == False]
  df = df[~df['intercom_last_seen_email'].str.endswith(".ie", na = False)]
  df = df[~df['intercom_last_seen_email'].str.endswith("ourresearch.org", na = False)]
  return df

@cache
def pkg_level_data_prep():
  sheet_pkg = gc.open('unsub-package-level')
  worksheet = sheet_pkg.get_worksheet(0)
  df = pd.DataFrame(worksheet.get_all_records())
  df = df[df['intercom_last_seen_email'].str.len() > 0]
  df['is_deleted'] = ['False' if not w else w for w in df['is_deleted']]
  df['is_deleted'] = [ast.literal_eval(w.title()) for w in df['is_deleted']]
  df['is_feeder_package'] = ['False' if not w else w for w in df['is_feeder_package']]
  df['is_feeder_package'] = [ast.literal_eval(w.title()) for w in df['is_feeder_package']]
  df['is_feedback_package'] = ['False' if not w else w for w in df['is_feedback_package']]
  df['is_feedback_package'] = [ast.literal_eval(w.title()) for w in df['is_feedback_package']]
  df['has_complete_counter_data'] = ['False' if not w else w for w in df['has_complete_counter_data']]
  df['has_complete_counter_data'] = [ast.literal_eval(w.title()) for w in df['has_complete_counter_data']]
  df['perpetual_access'] = ['False' if not w else w for w in df['perpetual_access']]
  df['perpetual_access'] = [ast.literal_eval(w.title()) for w in df['perpetual_access']]
  df['prices'] = ['False' if not w else w for w in df['prices']]
  df['prices'] = [ast.literal_eval(w.title()) for w in df['prices']]
  df['has_non_public_prices'] = ['False' if not w else w for w in df['has_non_public_prices']]
  df['has_non_public_prices'] = [ast.literal_eval(w.title()) for w in df['has_non_public_prices']]
  # remove any in a consortium where data is set up for them
  ## jisc, irel, crkn 
  df = df[~df['consortium_account'].str.contains("JISC|IreL|CRKN")]
  df = df[df['package_id'].str.contains("jisc", na = False) == False]
  df = df[df['institution_id'].str.contains("jisc", na = False) == False]
  df = df[~df['intercom_last_seen_email'].str.endswith(".ie", na = False)]
  df = df[~df['intercom_last_seen_email'].str.endswith("ourresearch.org", na = False)]
  return df

def remove_special_cases(df):
  return df[~df['intercom_last_seen_email'].str.contains('.+uccs.edu|.+nyu.edu')]

def inst_data(which = "not_using"):
  df = inst_level_data_prep()
  if which == "not_using":
    df = df[df['not_using']]
  else:
    df = df[df['new_users']]

  return remove_special_cases(df)

def pkgs_data(which = "required"):
  df = pkg_level_data_prep()
  
  if which == "required":
    return df[
      ~df['is_deleted'] & 
      df['missing_required_data'] & 
      ~df['is_feeder_package'] & 
      ~df['is_feedback_package'] &
      df['intercom_last_seen_email']
    ]
  else:
    df = df[
      ~df['is_deleted'] & 
      df['missing_recommended_data'] & 
      ~df['is_feeder_package'] & 
      ~df['is_feedback_package'] &
      df['intercom_last_seen_email']
    ]

  return remove_special_cases(df)

def contact_find(email):
  query = {'query': {'field': 'email', 'operator': '=', 'value': email}}
  res = requests.post(intercom_base + f"/contacts/search", json=query, headers=auth)
  if res.ok:
    data = res.json()
    if len(data['data']) > 1:
      # is there just 1 that has an Unsub database user id?
      exids = [w['external_id'] for w in data['data']]
      exids = list(filter(lambda w: w is not None, exids))
      if len(list(filter(lambda z: 'user-' in z, exids))) == 1:
        data['data'] = list(filter(lambda w: w['external_id'] is not None, data['data'])) 
        matched = list(filter(lambda w: 'user-' in w['external_id'], data['data']))
        user_id = matched[0]['id']
        external_id = matched[0]['external_id']
      else:
        # sort by last seen, then just pick the most recent
        try:
          last_seen = sorted(data['data'], key=lambda w: w['last_seen_at'])[-1]
          user_id = last_seen['id']
          external_id = last_seen['external_id']
        except:
          raise Exception(f"{email}: more than 1 match found for /contacts/search")
    else:
      user_id = data['data'][0]['id']
      external_id = data['data'][0]['external_id']
  else:
    raise Exception(f"{email}: failure for GET /contacts/search")

  return user_id, external_id

def create_req_message(group):
  mssgs = []
  for i,row in group.iterrows():
    if row['package_name']:
      criteria = {
        "COUNTER": not row['has_complete_counter_data'],
        "Prices": not row['prices'],
        "Currency": True if not row['currency'] else False,
        "Big Deal Cost": True if not row['big_deal_cost'] else False,
        "Big Deal Cost Increase": True if not row['big_deal_cost_increase'] else False,
      }
      criteria_missing = list(filter(lambda x: criteria[x], criteria))
      mssgs.append(f"  {row['package_name']} ({', '.join(criteria_missing)})")

  return '\n'.join(mssgs)

def create_reco_message(group):
  mssgs = []
  for i,row in group.iterrows():
    if row['package_name']:
      criteria = {
        "Post-Termination Access": not row['perpetual_access']
      }
      criteria_missing = list(filter(lambda x: criteria[x], criteria))
      mssgs.append(f"  {row['package_name']} ({', '.join(criteria_missing)})")

  return '\n'.join(mssgs)

def contact_update(dat, missing_dict):
  body = {
    "role": "user",
    "external_id": dat['external_id'],
    "email": dat['email'],
    "custom_attributes": missing_dict
  }
  json_headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
  res = requests.put(intercom_base + f"/contacts/{dat['id']}", json=body, headers=dict(auth, **json_headers))
  return res.ok

def missing_req_drop(x):
  print(f"    dropping from missing required {x['email']}")
  missing = {
    "missing_required_data": False,
    "missing_required_mssg": None
  }
  return contact_update(x, missing)

def missing_req_add(x, mssg):
  print(f"    adding to missing required {x['email']}")
  missing = {
    "missing_required_data": True,
    "missing_required_mssg": mssg
  }
  return contact_update(x, missing)

def missing_reco_drop(x):
  print(f"    dropping from missing recommended {x['email']}")
  missing = {
    "missing_recommended": False,
    "missing_recommended_mssg": None
  }
  return contact_update(x, missing)

def missing_reco_add(x, mssg):
  print(f"    adding to missing recommended {x['email']}")
  missing = {
    "missing_recommended": True,
    "missing_recommended_mssg": mssg
  }
  return contact_update(x, missing)

def not_using_drop(x):
  print(f"    dropping from not_using {x['email']}")
  missing = {"not_using": False}
  return contact_update(x, missing)

def not_using_add(x):
  print(f"    adding to not_using {x['email']}")
  data = {"not_using": True}
  return contact_update(x, data)

def new_users_drop(x):
  print(f"    dropping from new_users {x['email']}")
  data = {"new_user": False}
  return contact_update(x, data)

def new_users_add(x):
  print(f"    adding to new_users {x['email']}")
  data = {"new_user": True}
  return contact_update(x, data)

def contact_get(user_id):
  res = requests.get(intercom_base + f"/contacts/{user_id}", headers=auth)
  return res

def check_just_updated(user_id):
  updated_today = True
  out = contact_get(user_id)
  if out.ok:
    data = out.json()
    updated_today = date.today() == date.fromtimestamp(data['updated_at'])
  return updated_today

@cache
def intercom_contacts():
  results = []
  contacts_first = requests.get(intercom_base + "/contacts", headers=auth, params={'per_page': 150})
  if contacts_first.ok:
    contacts_first_dat = contacts_first.json()
    results.extend(contacts_first_dat['data'])
    starting_after = contacts_first_dat['pages']['next']['starting_after']
    while starting_after:
      try:
        print(starting_after)
        cts = requests.get(intercom_base + "/contacts", 
          headers=auth, params={'per_page': 150, 'starting_after': starting_after})
        tmpdat = cts.json()
        starting_after = tmpdat['pages']['next']['starting_after']
        results.extend(tmpdat['data'])
      except KeyError as e:
        starting_after = None

  return results

def intercom_clean(which = "required"):
  print("Getting all Intercom contacts")
  all_data = intercom_contacts()
  df_tmp = pkgs_data(which)

  print(f"Removing {which} data from Intercom")
  key_to_get = 'missing_required_data' if which == "required" else 'missing_recommended'
  icom_miss = list(filter(lambda w: w.get('custom_attributes', {}).get(key_to_get, {}), all_data))
  to_remove = list(filter(lambda x: x['email'] not in df_tmp['intercom_last_seen_email'].to_list(),
    icom_miss))
  
  # works fine with empty to_remove list
  match which:
    case "required":
      [missing_req_drop(w) for w in to_remove]
    case "recommended":
      [missing_reco_drop(w) for w in to_remove]
    case "not_using":
      [not_using_drop(w) for w in to_remove]
    case "new_users":
      [new_users_drop(w) for w in to_remove]

@click.group()
def cli():
  """
  Update intercom required and recommended data fields

  Examples:

    python intercom_drip.py --help

    python intercom_drip.py req --help

    python intercom_drip.py req

    python intercom_drip.py reco

    python intercom_drip.py not_using

    python intercom_drip.py new_users
  """

@cli.command(short_help='Update missing required data in Intercom')
def req():
  click.echo("Cleaning out required data in Intercom as needed")
  intercom_clean("required")
  
  click.echo("Updating required data in Intercom")
  df = pkgs_data("required")
  for email, group in df.groupby('intercom_last_seen_email'):
    print(f"  {email} (n={len(group)})")

    # get contact via search
    user_id, external_id = contact_find(email)

    # check if just updated, and skip if it was
    # just_updated = check_just_updated(user_id)
    # if just_updated:
    #   print(f"    {email} already updated today")
    #   continue

    # update contact attributes 
    mssg = create_req_message(group)

    # add data
    if mssg:
      dat = {'id': user_id, 'external_id': external_id, 'email': email}
      missing_req_add(dat, mssg)

@cli.command(short_help='Update missing recommended data in Intercom')
def reco():
  click.echo("Cleaning out recommended data in Intercom as needed")
  intercom_clean("recommended")
  
  click.echo("Updating recommended data in Intercom")
  df = pkgs_data("recommended")
  for email, group in df.groupby('intercom_last_seen_email'):
    print(f"  {email} (n={len(group)})")

    # get contact via search
    user_id, external_id = contact_find(email)

    # check if just updated, and skip if it was
    # just_updated = check_just_updated(user_id)
    # if just_updated:
    #   print(f"    {email} already updated today")
    #   continue

    # update contact attributes 
    mssg = create_reco_message(group)

    # add data
    if mssg:
      dat = {'id': user_id, 'external_id': external_id, 'email': email}
      missing_reco_add(dat, mssg)

@cli.command(short_help='Update not_using data in Intercom')
def not_using():
  click.echo("Cleaning out not using data in Intercom as needed")
  intercom_clean("not_using")
  
  click.echo("Updating not_using data in Intercom")
  df = inst_data("not_using")
  for email, group in df.groupby('intercom_last_seen_email'):
    print(f"  {email} (n={len(group)})")
    user_id, external_id = contact_find(email)
    dat = {'id': user_id, 'external_id': external_id, 'email': email}
    not_using_add(dat)

@cli.command(short_help='Update new_users data in Intercom')
def new_users():
  click.echo("Cleaning out not using data in Intercom as needed")
  intercom_clean("new_users")
  
  click.echo("Updating new_users data in Intercom")
  df = inst_data("new_users")
  for email, group in df.groupby('intercom_last_seen_email'):
    print(f"  {email} (n={len(group)})")
    user_id, external_id = contact_find(email)
    dat = {'id': user_id, 'external_id': external_id, 'email': email}
    new_users_add(dat)


if __name__ == "__main__":
  cli()



# # Add new metadata
# # gen = df_required.groupby('intercom_last_seen_email')
# # email = list(gen.groups.keys())[0]
# # group = gen.get_group(email)
# for email, group in df_required.groupby('intercom_last_seen_email'):
#   print(f"{email} (n={len(group)})")

#   # get contact via search
#   user_id, external_id = contact_find(email)

#   # check if just updated, and skip if it was
#   # just_updated = check_just_updated(user_id)
#   # if just_updated:
#   #   print(f"   {email} already updated today")
#   #   continue

#   # update contact attributes 
#   mssg = create_req_message(group)

#   # add data
#   if mssg:
#     dat = {'id': user_id, 'external_id': external_id, 'email': email}
#     missing_req_add(dat, mssg)
  
# delete just updated stuff 
# df_required = pkgs_data("required")
# for email, group in df_required.groupby('intercom_last_seen_email'):
#   print(f"{email} (n={len(group)})")
#   user_id, external_id = contact_find(email)
#   x = contact_get(user_id)
#   missing_req_drop(x.json())

# df_recommended = pkgs_data("recommended")
# for email, group in df_recommended.groupby('intercom_last_seen_email'):
#   print(f"{email} (n={len(group)})")
#   user_id, external_id = contact_find(email)
#   x = contact_get(user_id)
#   missing_reco_drop(x.json())
