# Release Notes

Unsub uses Continual Integration for releases: we are continually refining the production product, 
both its appearance and its underlying data.  
This means you will often see changes in the UI/UX, and sometimes in the presented data.  
Most of these changes are too small to note, but we'll keep a record of the larger ones here:

- 2022/08/19 Added support for all publishers! We've dropped support for specific publishers. Unsub packages are now essentially publisher agnostic. You can make Unsub packages with titles from 1 to any number of publishers. This change includes the ability to use Unsub for modeling aggregator packages as you may have through EBSCO or ProQuest. 
- 2022/08/19 APC report moved from the package level to institutional level. The report includes an estimate of your APC spend for only the five publishers Elsevier, Wiley, Taylor & Francis, Springer Nature, and SAGE.
- 2022/08/19 Packages now have descriptions. In addition, package and description editing are available in the list of packages under an institution and in the single package view under the Edit Details tab.
- 2022/08/19 Packages have a new optional filter step. A spreadsheet of ISSNs or a KBART file can be uploaded to filter the titles that appear in all scenario views within a package.
- 2022/08/04 Added support for bulk subscribe/unsubscribe to journals in Unsub scenrio dashboards. The upper right hand area of the dashboard has many changes to support this functionality: the search/select box is now always shown to make it more obvious that it's a feature users can take advantage of, the styling has changed, there are now two additional icons (shopping carts) to subscribe or unsubscribe to all selected journals.
- 2022/08/02 Fixed the pagination at the bottom of the table view in the Unsub scenario dashboard. The numbers shown before were not accurate; they are now!
- 2022/07/25 Fixes for PTA: 1) in the journal zoom Timeline tab, the correct years were not shown; now they are shown and right now show 2022 though 2026. 2) we were not including very recent PTA (e.g., end date Dec 2021); that is now fixed.
- 2022/07/25 Additional parts of the Unsub backend data are now from OpenAlex: number of articles, citations, authorships. Previously this data was from Microsoft Academic Graph. These changes lead to changes in all Unsub forecasts/dashboards, mostly for the better (higher fuflfillment at lower cost).
- 2022/06/17 Fix for demo accounts where dashboards weren't displaying correctly. Although we don't support any new demo accounts, some users have access to old demo accounts.
- 2022/06/17 Unsub dashboard improvement: The encircled question mark that previously linked to our documentation at help.unsub.org, now is a dropdown with links to documentation, webinars and a link to send us feedback at support@unsub.org
- 2022/06/14 Copying a scenario within a consortial package was broken due to a bug in our code. It is now fixed! 
- 2022/06/02 Integrate OpenAlex metadata into the Unsub backend. For now this includes journal metadata (e.g., ISSN, title, publisher, etc.) as well as journal concepts/subjects (e.g., Medicine, Ecology). Later we'll integrate additional OpenAlex data. To be clear, subjects throughout Unsub (in the user interface and in data exports) are now from OpenAlex. This change of journal metadata does affect what titles are included in your Unsub dashboards. We analyzed 212 scenarios before OpenAlex and after integrating OpenAlex, and there are only tiny differences: on average number of titles included in scenario are 0.62% different, forecasted costs are 0.18% different and forecasted access (fullfimment) is 0.05% different. 
- 2022/06/02 Scenario exports used to have two subject fields (subject, era_subjects), but now have three subject fields: subject, subject_top_three, and subjects_all. See the docs (https://docs.unsub.org/reference/data-export) for further details.
- 2022/03/23 Fix for Unsub dashboards: When a title's CPU was less than -1 the title (grey square) wasn't appearing in the histogram (but was present in the table view). Very few users were impacted by this. No Unsub backend changes - front end fix only.
- 2022/03/17 Fix in Unsub consortia dashboard: some consortia dashboards were displaying incorrect percent change in cost versus their big deal cost (see https://github.com/ourresearch/get-unsub/pull/28) 
- 2022/03/17 We have a new webinars page at https://unsub.org/webinars
- 2022/03/17 We made various improvements in our journal metadata; you may have noticed slight changes in your dashboards with respect to titles included or excluded following changes in our metadata; get in touch if you have any questions
- 2022/02/17 Fixed a problem where PTA and title price exports in the setup tabs failed if any ISSN's were missing - resulting from missing ISSN in user uploaded PTA or price files
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
