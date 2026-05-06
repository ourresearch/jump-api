-- One-time cleanup for issn_l values with leading/trailing whitespace in
-- openalex_computed. Goes hand in hand with the .strip() added in openalex.py
-- so freshly-built rows can no longer reintroduce the problem.
--
-- Strategy:
--   1. Identify any issn_l values where TRIM(x) != x.
--   2. For each, if a clean version already exists, delete the whitespace one.
--      Otherwise, UPDATE the whitespace one in place to the trimmed value.
--   3. Verify no whitespace remains.
--
-- Run this in a transaction. Read the SELECTs first; the DML is commented
-- out by default — uncomment after confirming the dry-run output matches
-- expectations.

-- 1. Inspect: which rows have whitespace issn_l, and is there a clean twin?
SELECT
  o.issn_l                                          AS dirty,
  TRIM(o.issn_l)                                    AS clean,
  EXISTS (SELECT 1 FROM openalex_computed o2
          WHERE o2.issn_l = TRIM(o.issn_l)
            AND o2.issn_l <> o.issn_l)              AS clean_twin_exists,
  o.publisher,
  o.is_gold_journal_in_most_recent_year,
  o.is_currently_publishing
FROM openalex_computed o
WHERE TRIM(o.issn_l) <> o.issn_l
ORDER BY o.issn_l;

-- 2. (uncomment to apply)
-- BEGIN;
--
-- -- Delete dirty rows where a clean twin already exists
-- DELETE FROM openalex_computed
-- WHERE TRIM(issn_l) <> issn_l
--   AND EXISTS (
--     SELECT 1 FROM openalex_computed o2
--     WHERE o2.issn_l = TRIM(openalex_computed.issn_l)
--       AND o2.issn_l <> openalex_computed.issn_l
--   );
--
-- -- For dirty rows without a clean twin, trim in place
-- UPDATE openalex_computed
-- SET issn_l = TRIM(issn_l)
-- WHERE TRIM(issn_l) <> issn_l;
--
-- -- Same for the concepts table, in case it has matching dupes
-- DELETE FROM openalex_concepts
-- WHERE TRIM(issn_l) <> issn_l
--   AND EXISTS (
--     SELECT 1 FROM openalex_concepts o2
--     WHERE o2.issn_l = TRIM(openalex_concepts.issn_l)
--       AND o2.issn_l <> openalex_concepts.issn_l
--   );
--
-- UPDATE openalex_concepts
-- SET issn_l = TRIM(issn_l)
-- WHERE TRIM(issn_l) <> issn_l;
--
-- -- Refresh the materialized view that depends on openalex_computed
-- REFRESH MATERIALIZED VIEW openalex_computed_flat;
--
-- COMMIT;

-- 3. Verify
SELECT COUNT(*) AS remaining_dirty
FROM openalex_computed
WHERE TRIM(issn_l) <> issn_l;
