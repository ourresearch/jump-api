# jump-api mapping

## Unsub: Code used in the Flask App (via views.py)

- data (directory of data)
- templates (email templates)
- requirements.txt (dependencies)
- admin_actions.py
- apc_journal.py
- app.py
- assumptions.py
- consortium.py
- consortium_journal.py
- counter.py
- emailer.py
- excel.py
- grid_id.py
- institution.py
- journal.py
- journal_price.py
- journalsdb.py
- package.py
- package_file_error_rows.py
- package_input.py
- password_reset.py
- permission.py
- perpetual_access.py
- prepared_demo_publisher.py
- raw_file_upload_object.py
- ror_id.py
- ror_search.py (not used, use in views commented out as of 2022-08-24)
- saved_scenario.py
- scenario.py
- user.py
- util.py
- views.py (main file running the Unsub backend, where routes are defined)

## Runs on Heroku, but not part of the Flask app

- consortium_calculate.py
- warm_cache.py
- parse_uploads.py

## Scripts

- save_groups.py
- rewrite_scenario_details.py
- backup_tables.py
- cleanup_tables.py
- consortium_recompute.py
- init_consortium.py
- init_institution.py
- init_n8.py
- n8_uni_result.py
- import_counter_from_json.py
- import_accounts.py
- init.py
- user_delete.py
- change_subs.py

## Heroku config

- Procfile
- runtime.txt
- newrelic.ini

## Documentation/etc

- README.md
- RELEASE_NOTES.md
- LICENSE
- docs/jump-api-mapping.md (this file)
- docs/bind-variables.md
- docs/running-unsub.md
- docs/caching.md

## Tests

- test/tests

## Leave for now, maybe remove later

- static (has unpaywall favicon, remove?)
- temp (remove maybe?)
- purge_cache.py
