# coding: utf-8

import requests
import argparse
import textwrap
from pathlib import Path
import pandas as pd
from saved_scenario import SavedScenario, save_raw_scenario_to_db

## Create test data
# import tempfile
# import csv
# scenario_id = 'fqUguFMB'
# path = tempfile.mkstemp(suffix='.csv')[1]
# data = [
# 	("Nature Chemistry","1755-4330",), 
# 	("Nature Neuroscience","1097-6256",), 
# 	("Nature Reviews Immunology","1474-1733",), 
# 	("Nature","0028-0836",), 
# 	("Nature Nanotechnology","1748-3387",),]
# with open(path, 'w') as out:
#     csv_out = csv.writer(out)
#     csv_out.writerow(['title','issn'])
#     for row in data:
#         csv_out.writerow(row)
# scenario_id = 'UTnjZXZR'

def smart_truncate(x, length=100, suffix=' ...'):
	strg = ', '.join(x)
	if len(strg) <= length:
		return strg
	else:
		return strg[:length] + suffix

def read_file(path):
	mypath = Path(path)
	try:
		mypath.resolve(strict=True)
	except FileNotFoundError as e:
		raise e
	else:
		df = pd.read_csv(path, sep=",")
		df.columns = map(str.lower, df.columns)
	
	if "issn" not in list(df.keys()):
		raise RuntimeError("path missing required column: 'issn'")

	return df

def issn_to_issnl(issns):
	finder_url = 'https://api.unpaywall.org/issn_ls'
	response = requests.post(finder_url, json = {'issns': issns})
	response.raise_for_status()
	dat = response.json()
	if not dat['issn_ls']:
		raise ValueError("no results found in Unpaywall ISSN-L finder API request")
	if len(dat['issn_ls']) != len(issns):
		raise ValueError("Unpaywall ISSN-L finder API results length ({}) not equal to input issns ({})".format(len(dat['issn_ls']), len(issns)))
	return dat['issn_ls']

def change_subs(scenario_id, path):
	data = read_file(path)
	issns = data['issn'].to_list()
	issns = issn_to_issnl(issns)
	scenario = SavedScenario.query.get(scenario_id)
	dict_to_save = scenario.to_dict_saved_from_db()
	dict_to_save["subrs"] = issns
	print("Setting subrs for {} to (n={}):\n {}\n".format(scenario_id, len(issns), smart_truncate(issns, length=50)))
	save_raw_scenario_to_db(scenario_id, dict_to_save, None)

if __name__ == "__main__":
	parser = argparse.ArgumentParser(
		formatter_class=argparse.RawDescriptionHelpFormatter,
		description=textwrap.dedent('''\
			Set subscriptions for a scenario to those from an input file (--path)

			Examples
			--------
			Notes:
				- prefix `heroku local:run` required to make sure environment variables are loaded
			
			# Show this help
			heroku local:run python change_subs.py -h
			# Change subscriptions with a scenario id and a file path
			heroku local:run python change_subs.py -i 29as8x9s9 -p data.csv
			'''))
	parser.add_argument("--id", help="An Unsub scenario id", type=str, required=True)
	parser.add_argument("--path", help="A path to a file with a column named 'issn'", type=str, required=True)
	args = parser.parse_args()
	change_subs(scenario_id = args.id, path = args.path)
