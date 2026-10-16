-- oxjob #1717: Unsub citation and authorship counts keyed on OpenAlex institution IDs, not GRID.
--
-- First-time order:
--   1. section 1 below
--   2. python openalex_institution_ids_to_unsub.py   fills openalex_ids + openalex_institution_lineage
--   3. section 2 below                                 Phase 1 sources, built from the frozen extract
--   4. python institution_metrics.py --all            fills jump_inst_authorship / jump_inst_citing
-- scenario.py reads the new tables once INSTITUTION_METRICS=openalex is set on Heroku.
-- Phase 2 replaces the three oa_work* sources with monthly Walden loads of the same shape.
-- Views are late-binding (with no schema binding) so the table swaps above don't break them.


-- 1. ROR -> OpenAlex mapping and lineage policy ------------------------------------------------

-- no row = the institution includes its OpenAlex lineage descendants
create table if not exists jump_institution_lineage_policy (
    institution_id varchar(256) not null,
    include_descendants boolean not null,
    note varchar(1000),
    updated timestamp default sysdate
) diststyle all;

-- parents of fellow members of the same consortium: lineage would count those members' papers
-- twice in consortium totals
insert into jump_institution_lineage_policy (institution_id, include_descendants, note) values
    ('institution-jisclon', false, 'Jisc member; parent of fellow Jisc members UCL, KCL, LSE, QMUL, Birkbeck and others'),
    ('institution-jiscnr7', false, 'Jisc member; parent of fellow Jisc members British Antarctic Survey, British Geological Survey'),
    ('institution-jiscang', false, 'Jisc member; parent of fellow Jisc member Writtle University College'),
    ('institution-jiscuwe', false, 'Jisc member; parent of fellow Jisc member Hartpury University');

create or replace view jump_institution_openalex_v1 as
    select distinct jr.institution_id, o.institution_id::bigint as openalex_id
    from public.jump_ror_id jr
    join public.openalex_ids o on o.ror_id = lower(regexp_replace(trim(jr.ror_id), '^https?://ror.org/', ''))
    where trim(jr.ror_id) <> ''
with no schema binding;


-- 2. Phase 1 sources, from the frozen extract (affiliations to Aug 2025, references to Jul 2022) --

-- work -> each affiliated OpenAlex institution and all its ancestors;
-- is_direct = an author is affiliated with openalex_id itself, not only with a descendant
create table oa_work_institution distkey(doi) sortkey(openalex_id) as
    select distinct a.doi, l.ancestor_id as openalex_id, (l.ancestor_id = a.orgid) as is_direct
    from public.ricks_affiliation_openalex a
    join public.openalex_institution_lineage l on l.institution_id = a.orgid
    join public.unpaywall u on u.doi = a.doi
    where u.year ~ '^[0-9]{4}$' and u.year >= '2019';

create or replace view oa_work as
    select doi, journal_issn_l as issn_l, case when year ~ '^[0-9]{4}$' then year::int end as year
    from public.unpaywall
    where coalesce(journal_issn_l, '') <> ''
with no schema binding;

create or replace view oa_work_refs as
    select citing_work_doi as citing_doi, cited_work_doi as cited_doi
    from public.openalex_paper_refs
with no schema binding;
