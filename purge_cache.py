import os
import argparse
import requests

import package
from app import db
from institution import Institution


def purge_common_package_data_cache(package_id):
    api_url = 'https://api.cloudflare.com/client/v4/zones/{}/purge_cache'.format(os.getenv('CLOUDFLARE_ZONE_ID'))

    cache_url = 'https://cdn.unpaywalljournals.org/live/data/common/{}?secret={}'.format(
        package_id,
        os.getenv('JWT_SECRET_KEY')
    )

    r = requests.post(
        api_url,
        headers={'Authorization': 'Bearer {}'.format(os.getenv('CLOUDFLARE_CACHE_API_KEY'))},
        json={'files': [cache_url]}
    )


def purge_all_caches(package_id):
    purge_common_package_data_cache(package_id)

    my_package = db.session.query(package.Package).filter(package.Package.package_id == package_id).scalar()
    if my_package:
        # my_package.clear_package_counter_breakdown_cache() # not used anymore
        pass
    else:
        print u"package not found: {}".format(package_id)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('package_id', help='package_id', type=str)

    purge_all_caches(parser.parse_args().package_id)
    print "done"
