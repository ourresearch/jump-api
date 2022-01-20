# HOW TO: Caching

A discussion about how caching is used in this repo.

## Used

As of 2022-01-20

### kids.cache

package on PyPi: https://pypi.org/project/kids.cache/

"Kids" ~ for "Keep It Dead Simple"

import: `from kids.cache import cache`
use via decorator: `@cache`

kids.cache can be used with methods, functions, and properties.

Warning from the maintainer of kids.cache: "the default cache store of
kids.cache is a standard dict, which is not recommended for long running
program with ever different queries as it would lead to an ever growing
caching store"

At some point we can think about changing out the caching store, e.g.
	
	from kids.cache imoprt cache
    from cachetools import LRUCache
    
    @cache(use=LRUCache(maxsize=2))
    def some_method ...

When to use: Can be used in most contexts (methods, functions, properties).
But from historical pattern, use `cached_property` for properties and
`kids.cache` for everything else.


### cached_property

package on PyPi: https://pypi.org/project/cached-property/

import: `from cached_property import cached_property`
use via decorator: `@cached_property`

Used for caching properties in classes

From the `cached_property.cached_property` docs: "A property that is only
computed once per instance and then replaces itself with an ordinary
attribute"

Each property with a `@cached_property` decorator within each instance of a
class created will then not need to evaluate the code in the property. So
within the context of Unsub, as long as an instance of a class is still
around with the same pointer on disk, the cached values will remain.

Nowhere in this codebase do we ever invalidate these cached properties
explicitly (e.g., using the `del` method described above). 

I couldn't find any documentation on where cached properties are stored. 
From what I can tell they are stored in the class itself.

When to use: For class properties only, so use for class properties as needed


### memorycache

`memorycache` is a decorator defined in `app.py`.

The only place `memorycache` is used is for the function
`get_common_package_data_for_all` in `scenario.py` (which eventually gets
called in `Scenario` class instantiation).

`memorycache` uses a dictionary (`my_memorycache_dict`) to store key and value
pairs. The key is formed from the function `__module__` and `__name__` plus
any additional `*args`. The value is whatever the decorated function returns.
The next time the function is called `memorycache` looks for the key in the
dictionary (`my_memorycache_dict`).

The associated function `reset_cache` - defined in `app.py` is used in two
places in the app: 

- in the `clear_caches` method of the `PackageInput` class
- in the `recompute_journal_dicts` method of the `Consortium` class

The above two uses are only invoked if the institution in question is a
consortium. That is, if the institution is stand-alone (not part of a
consortium), clear_caches is not invoked.

### warm_cache.py

`warm_cache.py` is one of the "process types" specified in the Procfile in
this repo. That is, it's run on Heroku as a separate process, just like web,
parse_uploads, and consortium_calculate.

The `warm_cache.py` script pings 13 Unsub endpoints
(e.g., `/institution/institution-Afxc4mAYXoJH`). This sort of constantly
runs, and it can be seen in the logs that we see requests from warm_cache
every couple of seconds on each Heroku dyno. 

The one comment I have from Heather is that this is related to Redshift.
Perhaps the idea was that requests to the Redshift databse would be faster if
the (presumably) database cache was more recently updated.

We may remove this. It's not totally clear why it's needed.



## Not Used

As of 2022-01-20

### warm_cache within app.py

There is a function `warm_cache` defined at the bottom of `app.py` that is
only used in an if block below it that AFAICT is never run because the env
var (`PRELOAD_LARGE_TABLES`) is not defined on Heroku.

### JSON response caching

There is no response caching, e.g. using Redis/Memcached. There used to
be at some point (using Memcached), but it was all pulled out (Heather
doesn't remember why). It's possible it was because the rate at which
users change aspects of scenarios is too fast to benefit from
response caching.

### s3_cache_get

`s3_cache_get` in `views.py`

There was apparently an attempt at using Amazon S3 to store and retrieve
cached JSON payloads. However, `s3_cache_get` is only used in commented out
code in `views.py`

### purge_cache.py

An unused script. Goal appears to be purging caches of the CDNs Cloudflare and
cdn.unpaywalljournals.org

File is to be deleted at some point

