import pandas as pd
from datetime import datetime, timedelta
from dateutil.parser import parse
import numpy as np

# inst_level = pd.read_csv("~/Downloads/unsub-institution-level.csv")
# all_institutions_df = pd.read_csv("~/Downloads/unsub-package-level.csv")

today = datetime.now()
one_month_ago = today - timedelta(days=30)
two_months_ago = today - timedelta(days=60)
three_months_ago = today - timedelta(days=90)
four_months_ago = today - timedelta(days=120)
six_months_ago = today - timedelta(days=182)
more_than_1_year_ago = today - timedelta(days=366)


# rules for institutions
def rule_not_paid(df_original):
	df = df_original.copy()
	df['intercom_last_seen'] = [parse(w) if isinstance(w, str) else w for w in df['intercom_last_seen']]
	df['date_last_paid_invoice'] = [parse(w) if isinstance(w, str) else w for w in df['date_last_paid_invoice']]
	df = df[~np.isnan(df['intercom_last_seen'])]

	# We noticed that you have been using Unsub but you’re payment is not up to date
	#- From columns: intercom_last_seen, current_deal, consortia, consortium_account, date_last_paid_invoice, amount_last_paid_invoice, created_sce_last
	#- Rule:
	# IF last seen on Intercom recently (last 3 mo's) 
	# AND there’s no current deal 
	# AND is not likely paid through consortium 
	# AND hasn’t paid an invoice within the last year

	not_paid = df[
		(df['intercom_last_seen'] > three_months_ago) &
		(~df['current_deal']) &
		(df['consortia'] == "No") &
		([not isinstance(w, str) for w in df['consortium_account'].to_list()]) &
		([z < more_than_1_year_ago if isinstance(z, pd.Timestamp) else True for z in df['date_last_paid_invoice'].to_list()])
	]
	not_paid = not_paid.assign(not_paid=True)

	df_original = df_original.assign(not_paid = df_original['institution_id'].map(not_paid.set_index('institution_id')['not_paid']))
	return df_original

def rule_not_using(df_original):
	df = df_original.copy()
	df['intercom_last_seen'] = [parse(w) if isinstance(w, str) else w for w in df['intercom_last_seen']]
	df['date_last_paid_invoice'] = [parse(w) if isinstance(w, str) else w for w in df['date_last_paid_invoice']]
	df['created_inst'] = [parse(w) if isinstance(w, str) else w for w in df['created_inst']]

	# We noticed that you have not been using Unsub despite having a current subscription
	#- From columns: intercom_last_seen, current_deal, consortia, consortium_account, date_last_paid_invoice, amount_last_paid_invoice, created_sce_last
	#- Rule:
	# IF NOT last seen on Intercom recently (last 3 mo's) 
	# AND 
	#   there is a recent deal 
	#   OR likely paid through consortium 
	#   OR paid an invoice within the last year
	# AND
	#   It's been more than 2 months since Institution created

	not_using = df[
		([z < three_months_ago if isinstance(z, pd.Timestamp) else True for z in df['intercom_last_seen'].to_list()]) &
		(
			df['current_deal'] | 
			[isinstance(w, str) and w != "No" for w in df['consortia']] |
			[isinstance(w, str) for w in df['consortium_account'].to_list()] |
			[z > more_than_1_year_ago if isinstance(z, pd.Timestamp) else False for z in df['date_last_paid_invoice'].to_list()]
		) &
		(df['created_inst'] < two_months_ago)
	]
	not_using = not_using.assign(not_using=True)

	df_original = df_original.assign(not_using = df_original['institution_id'].map(not_using.set_index('institution_id')['not_using']))
	return df_original

def rule_new_users(df_original):
	df = df_original.copy()
	df['created_inst'] = [parse(w) if isinstance(w, str) else w for w in df['created_inst']]

	# Follow up with new users: > 1 month & < 6 months
	new_users = df[(df['created_inst'] < one_month_ago) & (df['created_inst'] > six_months_ago)]
	new_users = new_users.assign(new_users=True)

	df_original = df_original.assign(new_users = df_original['institution_id'].map(new_users.set_index('institution_id')['new_users']))
	return df_original



# rules for packages
def rule_required_data(df_original):
	df = df_original.copy()

	# We noticed you haven’t uploaded required data
	# From columns: is_deleted, is_feeder_package, is_feedback_package, currency, big_deal_cost, big_deal_cost_increase, has_complete_counter_data
	# Rule:
	# IF NOT is_deleted
	# AND NOT is_feeder_package
	# AND NOT is_feedback_package
	# ~~AND is current_user~~ (column doesn't exist yet)
	# AND 
	#    currency is empty OR 
	#    big_deal_cost is empty OR
	#    big_deal_cost_increase is empty OR 
	#    has_complete_counter_data=FALSE
	mrd = df[(
		([not w if isinstance(w, bool) else False for w in df['is_deleted']]) and
		([not w if isinstance(w, bool) else False for w in df['is_feeder_package']]) and
		([not w if isinstance(w, bool) else False for w in df['is_feedback_package']]) and
		(
			df['currency'].isnull() |
			df['big_deal_cost'].isnull() |
			df['big_deal_cost_increase'].isnull() |
			([not w if isinstance(w, bool) else False for w in df['has_complete_counter_data']])
		)
	)]
	mrd = mrd.assign(missing_required_data=True)

	df_original = df_original.merge(mrd[['package_id','ror_id','missing_required_data']], how='left', on=['package_id','ror_id'])
	return df_original

def rule_recommended_data(df_original):
	df = df_original.copy()

	# We noticed you haven’t uploaded recommended data
	# From columns: is_deleted, is_feeder_package, is_feedback_package, perpetual_access, custom_price
	# Rule:
	# IF NOT is_deleted 
	# AND NOT is_feeder_package 
	# AND NOT is_feedback_package 
	# ~~AND is current_user~~ (column doesn't exist yet)
	# AND 
	#     perpetual_access=FALSE OR
	#     custom_price=FALSE
	mrecd = df[(
		([not w if isinstance(w, bool) else False for w in df['is_deleted']]) and
		([not w if isinstance(w, bool) else False for w in df['is_feeder_package']]) and
		([not w if isinstance(w, bool) else False for w in df['is_feedback_package']]) and
		(
			([not w if isinstance(w, bool) else False for w in df['perpetual_access']]) or
			([not w if isinstance(w, bool) else False for w in df['custom_price']])
		)
	)]
	mrecd = mrecd.assign(missing_recommended_data=True)

	df_original = df_original.merge(mrecd[['package_id','ror_id','missing_recommended_data']], how='left', on=['package_id','ror_id'])
	return df_original


