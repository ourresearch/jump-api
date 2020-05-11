import os

import requests


def purge_the_cache(package_id):
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