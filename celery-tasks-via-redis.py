import os
import redis
from urllib.parse import urlparse
import json
import re

url = urlparse(os.environ['REDIS_URL'])
r = redis.Redis(host=url.hostname, port=url.port, username=url.username, password=url.password, ssl=True, ssl_cert_reqs=None)
keys = r.keys('celery*')
keys
# r.get(keys[1])

messages = []
for key in keys:
	mssg = r.get(key)
	mssg_dict = json.loads(mssg)
	if mssg_dict['status'] == "SUCCESS":
		mssg_dict['package_id'] = re.search('package-.+', mssg_dict['result']).group(0)
	messages.append(mssg_dict)

messages = sorted(messages, key=lambda x: x['date_done'])
messages
