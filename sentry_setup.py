# coding: utf-8
"""Sentry error reporting (oxjob #1685).

Imported at the top of app.py (web, parse_uploads, consortium_calculate and any script that
imports app) and of warm_cache.py. A missing SENTRY_DSN makes it a no-op. Errors only: no
tracing. logger.error/exception lines become events, since this app logs only real failures
there (email send, DB commit).
"""
import os
import re

import sentry_sdk
from sentry_sdk.scrubber import DEFAULT_DENYLIST, EventScrubber

# Unsub passes JWTs as ?jwt=... (warm_cache, links), a key Sentry's scrubbers don't know.
_JWT_PARAM = re.compile(r"(jwt=)[^&#\s'\"]+")


def _scrub_jwt(value):
    if isinstance(value, str):
        return _JWT_PARAM.sub(r"\1[Filtered]", value)
    if isinstance(value, dict):
        return {k: _scrub_jwt(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_scrub_jwt(v) for v in value]
    return value


if os.environ.get("SENTRY_DSN"):
    sentry_sdk.init(
        dsn=os.environ["SENTRY_DSN"],
        environment=os.environ.get("SENTRY_ENVIRONMENT", "production"),
        release=os.environ.get("HEROKU_SLUG_COMMIT") or os.environ.get("HEROKU_RELEASE_VERSION"),
        server_name=os.environ.get("DYNO"),
        send_default_pii=False,
        traces_sample_rate=0,
        before_send=lambda event, hint: _scrub_jwt(event),  # also covers breadcrumbs
        event_scrubber=EventScrubber(denylist=DEFAULT_DENYLIST + ["jwt"], recursive=True),
    )
