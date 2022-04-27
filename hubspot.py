import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

from hubspot3.companies import CompaniesClient
from hubspot3.deals import DealsClient
from hubspot3.crm_associations import CRMAssociationsClient
from hubspot3.contacts import ContactsClient

def int_try(x):
	try:
	    z = int(x)
	    x = z
	finally:
		return x

class HubSpot(object):
	def __init__(self):
		super(HubSpot, self).__init__()
		self.HS_API_KEY = os.environ.get('HS_API_KEY')
		self.company_client = CompaniesClient(api_key=self.HS_API_KEY)
		self.deals_client = DealsClient(api_key=self.HS_API_KEY)
		self.assoc_client = CRMAssociationsClient(api_key=self.HS_API_KEY)
		self.contacts_client = ContactsClient(api_key=self.HS_API_KEY)
		self._companies = []
		self._deals = []
		self._contacts = []

	def fetch_companies(self, extra_properties=["ror_id","consortia","consortium_account","amount_last_paid_invoice","date_last_paid_invoice","domain"]):
		print("fetching companies")
		self._companies = self.company_client.get_all(extra_properties=extra_properties)

	def fetch_deals(self, extra_properties=["our_research_deal_type"]):
		print("fetching deals")
		self._deals = self.deals_client.get_all(extra_properties=extra_properties)

	def fetch_contacts(self, extra_properties=[]):
		print("fetching contacts")
		self._contacts = self.contacts_client.get_all(extra_properties=extra_properties)
	
	def extract_deals(self, company_id):
		company_deals = list(filter(lambda x: company_id in x['associatedCompanyIds'], self._deals))
		deal_data = []
		for deal in company_deals:
			closedate = deal.get('closedate')
			createdate = deal.get('createdate')
			if closedate:
				closedate = datetime.fromtimestamp(int(closedate)/1000)
			if createdate:
				createdate = datetime.fromtimestamp(int(createdate)/1000)
			amount = deal.get('amount')
			if amount:
				amount = int_try(amount)
			deal_data.append({'close_date': closedate, 'create_date': createdate,
				'amount': amount, 'stage': deal.get('dealstage'),
				'our_research_deal_type': deal.get('our_research_deal_type'),})
		# sort by close dates
		deal_data = sorted(deal_data, key = lambda x: (x['close_date'] is None, x['close_date']))
		return deal_data

	def companies(self):
		self.fetch_companies()
		self.fetch_deals()
		print("matching deals to companies")
		for company in self._companies:
			# print(company['id'])
			company['deals'] = self.extract_deals(company_id=company['id'])

	def filter_by_ror_id(self, ror_id):
		if self._companies:
			return list(filter(lambda x: x.get('ror_id') == ror_id, self._companies))

	def current_deal(self, ror_id):
		customer = False # by default not a current customer
		data = self.filter_by_ror_id(ror_id)
		try:
			if data: # company not found
				data = data[0]
				deals = data['deals']
				if deals: # no deals found
					deals_closed_won = list(filter(lambda x: x['stage'] == "closedwon", deals))
					if deals_closed_won:
						date_max = max([w['close_date'] for w in deals_closed_won if w['close_date'] is not None])
						deal = list(filter(lambda x: x['close_date'] == date_max, deals))[0]
						if deal['our_research_deal_type'] == "UJD": # only Unsub deals
							if deal['close_date']: # no closed date
								end_date = deal['close_date'] + relativedelta(years=1)
								customer = end_date > datetime.today()
		except:
			pass

		return customer

# hs.fetch_companies()
# hs.fetch_contacts()
# hs._deals
# hs._companies[0:4]
# hs.filter_by_ror_id(ror_id='asdff')

# not a current customer
# hs.filter_by_ror_id(ror_id='05g3dte14')
# hs.current_deal(ror_id='05g3dte14')

# # not a current customer: JULAC
# hs.filter_by_ror_id(ror_id='00t33hh48')
# hs.current_deal(ror_id='00t33hh48')

# # is a current customer
# hs.filter_by_ror_id(ror_id='01vx35703')
# hs.current_deal(ror_id='01vx35703')

# # consortium members: CRKN
# hs.filter_by_ror_id(ror_id='010gxg263')
# hs.current_deal(ror_id='010gxg263')

# # consortium JISC
# hs.filter_by_ror_id(ror_id='01rv9gx86')
# hs.current_deal(ror_id='01rv9gx86')
