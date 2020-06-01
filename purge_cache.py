import os

import requests

import package
from app import db
from institution import Institution
from scenario import refresh_perpetual_access_from_db, refresh_cached_prices_from_db


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
    refresh_perpetual_access_from_db(package_id)

    my_package = db.session.query(package.Package).filter(package.Package.package_id == package_id).scalar()
    my_package.clear_package_counter_breakdown_cache()
    refresh_cached_prices_from_db(my_package.package_id, my_package.publisher)
