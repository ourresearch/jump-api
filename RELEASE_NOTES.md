# Release Notes

Unpaywall Journals uses Continual Integration for releases: we are continually refining the production product, 
both its appearance and its underlying data.  
This means you will often see changes in the UI/UX, and sometimes in the presented data.  
Most of these changes are too small to note, but we'll keep a record of the larger ones here:

- 2020/01/10: Bug fixes for journals that have been publishing for fewer than five years, changing their fulfillment and cost estimates.
- 2020/01/10: Improved OA estimates for views of articles that are more than five years old, raising overall OA rates and in some cases lowering backfile rates.
- 2020/01/09: Added new documentation:
-- [Journals tab](https://support.unpaywall.org/a/solutions/articles/44001872900)
-- [What does "Edit Subscriptions" do and how does it work?](https://support.unpaywall.org/a/solutions/articles/44001872930)
-- [How do we calculate ILL requests and ILL cost?](https://support.unpaywall.org/support/solutions/articles/44001872901)
- 2020/01/08: Added breakdown of COUNTER file to the Packages page ([screenshot](https://i.imgur.com/9En4Zhx.png))
- 2020/01/07: Added a config parameter for Backfile Percentage Available as Perpetual Access ([docs](https://support.unpaywall.org/a/solutions/articles/44001822208))
- 2020/01/02: Updated the database to deduplicate a few journals: now at 1855 journals for most Elsevier packages
- 2019/12/01: Added [NCPPU](https://support.unpaywall.org/support/solutions/articles/44001822684) Rank column
- 2019/12/01: Improved modeling to include the growth (or decline) in number of articles published in each journal over the next five years, based on growth curve of the last five years.  Previously assumed the number of articles would be constant.
- 2019/12/01: Improved modeling to use smoothed data for each journal's download-by-age curve (fits an exponential curve) to decrease noise.  Previously used raw data, or an averaged curve across all journals when too erratic.
- 2019/11/20: Created a new tab named Journals, which lets you add and remove columns from the journal table.  Removed tabs that were named after specific column sets.

These release notes are copied to the knowledge base here: https://support.unpaywall.org/a/solutions/articles/44001853287