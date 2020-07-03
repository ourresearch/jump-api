# Release Notes

Unsub uses Continual Integration for releases: we are continually refining the production product, 
both its appearance and its underlying data.  
This means you will often see changes in the UI/UX, and sometimes in the presented data.  
Most of these changes are too small to note, but we'll keep a record of the larger ones here:

- 2020/07/03 Scenario now only includes journals in the uploaded COUNTER file
- 2020/07/01 Fixed a modelling bug in custom prices: now use custom prices directly rather than as minimum with public price
- 2020/06/17 Fixed a bug in perpetual access display:  don't show as <2010 because it sorts badly
- 2020/06/17 Fixed a bug in custom perpetual access calculations when PA started recently and no end date given
- 2020/06/04 Fixed a bug in the estimate of the older-than-five-years use of very low-use journals.  Data will notably change for a few very low-use journals.
- 2020/06/03 Data in your Elsevier model was updated to February (pre-covid) -- the OA rate will go up about 1-2% for most universities. Cost-per-use will change noticeably for a few journals, but it will remain about the same most. 