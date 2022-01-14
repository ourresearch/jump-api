# Release Notes

Unsub uses Continual Integration for releases: we are continually refining the production product, 
both its appearance and its underlying data.  
This means you will often see changes in the UI/UX, and sometimes in the presented data.  
Most of these changes are too small to note, but we'll keep a record of the larger ones here:

- 2022/01/14 Another fix for quotes in scenario names: all double quotes replaced with single quotes to avoid errors
- 2022/01/14 Many changes throughout the Unsub backend to use proper SQL bind variables, including addition of many tests, and tweaks to allow testing on test database, test S3 buckets, and staging heroku instance. None of these changes are user facing.
- 2021/11/23 Fixed a bug that was causing user permissions to be removed from an institution when their role was changed in the user dashboard. 
- 2021/10/25 Fixed two bugs: 1) When creating a new scenario, the name given was sometimes not being used. 2) When creating a new scenario or renaming a scenario, scenario names with quotes were throwing errors. 
- 2021/10/14 Fixed a bug due to some journal titles having incorrect titles, in particular titles staring with "Nature Reviews"
- 2021/09/23 Moved from using Python 2 to using Python 3
- 2021/06/20 Fix bug in authorships and citations that has been live for the last ten days
- 2021/05/17 Many updates, including COUNTER 5 support, GBP support, Perpetual Access (PTA) changes, and more.  See [Release Notes](http://help.unsub.org/en/articles/5238375-release-notes-may-2021). 
- 2021/05/12 Updating Unsub's journal lists (publicly available prices, which journals are published by which publishers, which journals have flipped to OA, which journals are new, etc).  The forecasts and APC calculations on your dashboards may update slightly as a result.  
- 2020/12/13 Fixed bug for overriding custom big deal price for universities in consortia
- 2020/12/10 Was requiring a publisher match to include OA in forecast; now only requires an ISSN match
- 2020/12/09 In the upload tab, show the sum of all COUNTER data for the same issn_l
- 2020/12/08 Stop excluding journals in new uploads because of mismatched publisher
- 2020/12/02 Add support for SAGE and T&F
- 2020/12/02 Fix bug where projected downloads could sometimes be negative: affects very few journals
- 2020/09/23 Bug fix to perpetual access. See [announcement post](https://groups.google.com/g/unsub-announce/c/yaml_UADHa0).
- 2020/09/10 Allow custom journal prices to be 0 and journal to still appear in forecast
- 2020/07/27 Allow fuzzy publisher matching for custom prices (some custom prices may be applied that were skipped before)
- 2020/07/23 Update publishers for about 25 journals that recently moved to Elsevier and Wiley (will make them show up in some scenarios for the first time)
- 2020/07/23 Merge 'Journal of Applied Corporate Finance' which will make them show up in some scenarios for the first time
- 2020/07/12 Rename a few columns in export (documented in Knowledge Base)
- 2020/07/03 Scenario now only includes journals in the uploaded COUNTER file
- 2020/07/01 Fixed a modelling bug in custom prices: now use custom prices directly rather than as minimum with public price
- 2020/06/17 Fixed a bug in perpetual access display:  don't show as <2010 because it sorts badly
- 2020/06/17 Fixed a bug in custom perpetual access calculations when PA started recently and no end date given
- 2020/06/04 Fixed a bug in the estimate of the older-than-five-years use of very low-use journals.  Data will notably change for a few very low-use journals.
- 2020/06/03 Data in your Elsevier model was updated to February (pre-covid) -- the OA rate will go up about 1-2% for most universities. Cost-per-use will change noticeably for a few journals, but it will remain about the same most. 
