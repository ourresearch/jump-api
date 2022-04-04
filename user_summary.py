# Run like:
## heroku run --size=performance-l python user_summary.py -r heroku
# Or run in the background like:
## heroku run:detached --size=performance-l python user_summary.py -r heroku

import pandas as pd
import numpy as np
import os
import json
import gspread
from datetime import datetime

from app import get_db_cursor
from package import Package

from hubspot import HubSpot
from intercom import intercom

hs = HubSpot()
hs.companies()

def params_changed(x):
	defaults = {
	 'cost_bigdeal_increase': 5.0,
	 'cost_alacart_increase': 8.0,
	 'cost_content_fee_percent': 5.7,
	 'cost_ill': 17.0,
	 'ill_request_percent_of_delayed': 5.0,
	 'weight_citation': 10.0,
	 'weight_authorship': 100.0,
	 'include_bronze': True, # bronze OA
	 'include_social_networks': True, # research gate OA
	 'include_submitted_version': True, # green OA
	}
	x_filt = {key: x[key] for key in list(defaults.keys())}
	differs_log = defaults != x_filt
	diff_dict = {k: x_filt[k] for k in x_filt if k in x_filt and defaults[k] != x_filt[k]}
	return differs_log, diff_dict

# Get institution ids that have Unsub users w/ permissions access
with get_db_cursor() as cursor:
    cmd = """select distinct(ji.id),display_name,created,is_consortium,consortium_id,ror.ror_id
		from jump_institution as ji
		join jump_user_institution_permission as juip on ji.id=juip.institution_id
		join jump_ror_id as ror on ji.id=ror.institution_id
		where not ji.is_demo_institution;
	"""
    cursor.execute(cmd)
    rows = cursor.fetchall()

institutions = pd.DataFrame(rows, columns=['institution_id','name','created','is_consortium','consortium_id','ror_id'])

# Consortia
institutions['is_consortium'].fillna(False, inplace=True)
consortia = institutions[institutions['is_consortium']]
## drop demo/test accounts
consortia = consortia[~consortia['name'].str.contains("Demo|Testing")]

# Non-consortia
non_consortia = institutions[~institutions['is_consortium']]
## exclude demo/test institutions
non_consortia = non_consortia[~non_consortia['institution_id'].str.contains("institution-testing")]
non_consortia = non_consortia[~non_consortia['institution_id'].str.contains("institution-demo")]
non_consortia = non_consortia[~non_consortia['name'].str.contains("Demo")]
non_consortia = non_consortia[~non_consortia['name'].str.contains("Test")]
non_consortia = non_consortia[~non_consortia['name'].str.contains("Scott")]


# Each institution
# institution="institution-jscQRozbejja"
# it = non_consortia[0:20].iterrows()
# row = next(it)[1]
# non_consortia.iterrows()[572]
all_institutions = []
for index, row in non_consortia.iterrows():
	print(row["ror_id"])
	with get_db_cursor() as cursor:
	    cmd = "select * from jump_account_package where institution_id = %s"
	    cursor.execute(cmd, (row["institution_id"],))
	    rows_inst = cursor.fetchall()

	if not rows_inst:
		institution_pkgs = pd.DataFrame({"institution_name": row["name"], 'ror_id':row["ror_id"]}, 
			index = [0])
		# institution_pkgs["current_deal"] = hs.current_deal(ror_id=row["ror_id"])
		# company = hs.filter_by_ror_id(ror_id=row["ror_id"])
	else:
		institution_pkgs = pd.DataFrame(rows_inst, columns=['account_id','package_id','publisher','package_name','created','con_package_id','institution_id','is_demo','big_deal_cost','is_deleted','updated','default_to_no_perpetual_access','currency','is_dismissed_warning_missing_perpetual_access','is_dismissed_warning_missing_prices','big_deal_cost_increase'])
		institution_pkgs.drop(["account_id","con_package_id","is_dismissed_warning_missing_perpetual_access","is_dismissed_warning_missing_prices","default_to_no_perpetual_access","updated"], axis=1, inplace=True)
		institution_pkgs["institution_name"] = row["name"]
		institution_pkgs["ror_id"] = row["ror_id"]

	institution_pkgs["current_deal"] = hs.current_deal(ror_id=row["ror_id"])
	company = hs.filter_by_ror_id(ror_id=row["ror_id"])
	consortia = None
	consortium_account = None
	date_last_paid_invoice = None
	amount_last_paid_invoice = None
	if company:
		consortia = company[0].get('consortia')
		consortium_account = company[0].get('consortium_account')
		dlpi = company[0].get('date_last_paid_invoice')
		date_last_paid_invoice = datetime.strptime(dlpi, '%m/%d/%Y').strftime("%Y-%m-%d") if dlpi else None
		alpi = company[0].get('amount_last_paid_invoice')
		amount_last_paid_invoice = float(alpi) if alpi else None 
	institution_pkgs["consortia"] = consortia
	institution_pkgs["consortium_account"] = consortium_account
	institution_pkgs["date_last_paid_invoice"] = date_last_paid_invoice
	institution_pkgs["amount_last_paid_invoice"] = amount_last_paid_invoice
	institution_pkgs["created_inst"] = row['created'].strftime("%Y-%m-%d")

	# intercom
	intlastseen = None
	emaillastseen = None
	with get_db_cursor() as cursor:
	    cmd = "select * from jump_debug_admin_combo_view where institution_id = %s"
	    cursor.execute(cmd, (row["institution_id"],))
	    rows_users = cursor.fetchall()
	if rows_users:
		emails = list(filter(lambda x: x is not None, [w['email'] for w in rows_users]))
		domain = None
		if company:
			domain = company[0].get('domain')
		intlastseen, emaillastseen = intercom(emails, domain)

	institution_pkgs["intercom_last_seen"] = intlastseen
	institution_pkgs["intercom_last_seen_email"] = emaillastseen
	# end intercom

	# packages
	pkgid = institution_pkgs.get('package_id')
	if not isinstance(pkgid, pd.Series):
		all_institutions.append(institution_pkgs)
	else:
		pkg_ids = pkgid.to_list()
		pkg_dict_list = []
		# This is the slow part: queries for each package
		for pkg in pkg_ids:
			try:
				pkg = Package.query.get(pkg)
				mpnum = 0		
				mp = list(filter(lambda x: x['id'] == "missing_prices", pkg.warnings))
				if len(mp):
					mpnum = len(mp[0]['journals'])
				saved_scenarios = pkg.saved_scenarios
				scenario_configs = [params_changed(w.to_dict_definition()['configs']) for w in saved_scenarios]
				scenario_dates = [w.created for w in saved_scenarios]
				scenario_dates.sort()
				pkg_dict_list.append({"package_id":pkg.package_id,
					# "created_pkg": pkg.created,
					"has_complete_counter_data": pkg.has_complete_counter_data,
					"perpetual_access": pkg.data_files_dict['perpetual-access']['is_live'],
					"custom_price": pkg.data_files_dict['price']['is_live'],
					"missing_prices": mpnum,
					"is_feeder_package": pkg.is_feeder_package,
					"is_feedback_package": pkg.is_feedback_package,
					"scenarios": len(pkg.scenario_ids),
					"scenario_user_subrs": any([len(w.to_dict_definition()['subrs']) > 0 for w in pkg.saved_scenarios]),
					"scenario_param_chgs": any([x[0] for x in scenario_configs]),
					"scenario_param_str": ",".join([str(x[1]) for x in scenario_configs]),
					"created_sce_first": min(scenario_dates).strftime("%Y-%m-%d") if scenario_dates else None,
					"created_sce_last": max(scenario_dates).strftime("%Y-%m-%d") if scenario_dates else None, })	
			except Exception as e:
				pkg_dict_list.append({})

		pkg_details = pd.DataFrame(pkg_dict_list)
		all_institutions.append(institution_pkgs.merge(pkg_details, on="package_id"))

# len(all_institutions)
# all_institutions
all_institutions_df = pd.concat(all_institutions)
created_pkg_new = [w.strftime("%Y-%m-%d") if isinstance(w, pd.Timestamp) else w for w in all_institutions_df['created'].to_list()]
del all_institutions_df['created']
all_institutions_df['created_pkg'] = created_pkg_new
all_institutions_df = all_institutions_df[["institution_id","institution_name","ror_id","created_inst","current_deal","consortia","consortium_account","date_last_paid_invoice","amount_last_paid_invoice",
	"intercom_last_seen", "intercom_last_seen_email",
	"package_id","package_name","created_pkg","publisher","is_deleted",
	"currency","big_deal_cost","big_deal_cost_increase","has_complete_counter_data",
	"perpetual_access","custom_price","is_feeder_package","is_feedback_package",
	"created_sce_first", "created_sce_last",
	"scenarios", "scenario_user_subrs", "scenario_param_chgs", "scenario_param_str",]]
pkg_file = 'non_consortia_pkg_level.csv'
all_institutions_df.to_csv(pkg_file, index=False)



# aggregate package level data up to institutions
inst_level = all_institutions_df.copy()
inst_level = inst_level[~inst_level['is_deleted'].fillna(False) & ~inst_level['is_feeder_package'].fillna(False) & ~inst_level['is_feedback_package'].fillna(False)]
inst_level['created_sce_last'] = pd.to_datetime(inst_level['created_sce_last'])
inst_level = pd.concat([
	inst_level.groupby(['institution_id','institution_name'])['ror_id'].apply(lambda x: ",".join(list(np.unique(x)))),
	inst_level.groupby(['institution_id','institution_name'])['created_inst'].apply(lambda x: ",".join(list(np.unique(x)))),
	inst_level.groupby(['institution_id','institution_name'])['current_deal'].apply(lambda x: list(np.unique(x))[0]),
	inst_level.groupby(['institution_id','institution_name'])['consortia'].apply(lambda x: ",".join(filter(None, list(set(x))))),
	inst_level.groupby(['institution_id','institution_name'])['consortium_account'].apply(lambda x: ",".join(filter(None, list(set(x))))),
	inst_level.groupby(['institution_id','institution_name'])['date_last_paid_invoice'].apply(lambda x: ",".join(filter(None, list(set(x))))),
	inst_level.groupby(['institution_id','institution_name'])['amount_last_paid_invoice'].apply(lambda x: list(set(x))[0]),
	inst_level.groupby(['institution_id','institution_name'])['intercom_last_seen'].apply(lambda x: ",".join(filter(None, list(set(x))))),
	inst_level.groupby(['institution_id','institution_name'])['intercom_last_seen_email'].apply(lambda x: ",".join(filter(None, list(set(x))))),
	inst_level.groupby(['institution_id','institution_name'])['publisher'].apply(lambda x: ",".join(list(np.unique(list(filter(lambda z: isinstance(z, str), x)))))),
	inst_level.groupby(['institution_id','institution_name']).nunique().package_id,
	inst_level.groupby(['institution_id','institution_name']).sum().scenarios,
	inst_level.groupby(['institution_id','institution_name'])['has_complete_counter_data'].all(),
	inst_level.groupby(['institution_id','institution_name'])['perpetual_access'].all(),
	inst_level.groupby(['institution_id','institution_name'])['custom_price'].all(),
	inst_level.groupby(['institution_id','institution_name'])['created_sce_last'].max(),
	inst_level.groupby(['institution_id','institution_name'])['scenario_user_subrs'].any(),
	inst_level.groupby(['institution_id','institution_name'])['scenario_param_chgs'].any(),
], axis = 1).reset_index()
inst_level.rename(columns={
	'publisher': 'publishers', 
	'package_id': 'no_pkgs', 
	'scenarios': 'no_scenarios', 
	'has_complete_counter_data': 'any_wo_counter_data', 
	'perpetual_access': 'any_wo_pta', 
	'custom_price': 'any_wo_custom_prices',
	'scenario_user_subrs': 'any_scenario_user_subrs',
	'scenario_param_chgs': 'any_scenario_param_chgs'}, inplace=True)
inst_file = 'non_consortia_inst_level.csv'
inst_level.to_csv(inst_file, index=False)


# apply rules
from user_summary_rules import rule_not_paid, rule_not_using, rule_new_users
inst_with_rules = rule_not_paid(inst_level)
inst_with_rules = rule_not_using(inst_with_rules)
inst_with_rules = rule_new_users(inst_with_rules)
inst_with_rules.to_csv(inst_file, index=False)

from user_summary_rules import rule_required_data, rule_recommended_data
pkg_with_rules = rule_required_data(all_institutions_df)
# pkg_with_rules = rule_recommended_data(pkg_with_rules)
pkg_with_rules.to_csv(pkg_file, index=False)


# Upload to google sheets 
json_cred = os.environ.get('GOOGLE_SHEETS_JSON')
file_cred = 'google_sheets_creds.json'
with open(file_cred, 'w') as f:
    json.dump(json.loads(json_cred), f, ensure_ascii=False, indent=4)
gc = gspread.service_account(file_cred)

# get sheets
sheet_inst = gc.open('unsub-institution-level')
sheet_pkg = gc.open('unsub-package-level')

# write new data to sheets
if os.path.isfile(inst_file):
	with open(inst_file, 'r') as x:
		txt_inst = x.read().encode('UTF-8')
		gc.import_csv(sheet_inst.id, txt_inst)

if os.path.isfile(pkg_file):
	with open(pkg_file, 'r') as x:
		txt_pkg = x.read().encode('UTF-8')
		gc.import_csv(sheet_pkg.id, txt_pkg)

# cleanup
try:
    os.remove(inst_file)
except OSError:
    pass

try:
    os.remove(pkg_file)
except OSError:
    pass    
