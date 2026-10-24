-- Row filters and column masks. Groups (mapped 1:1 from Entra ID via SCIM in prod):
--   data_platform        engineers + pipeline/job service principals (full access, PII included)
--   mgr_<cc>             country managers: their country only, detailed gold, PII masked
--   pii_reader_<cc>      may see raw loyalty ID / email for that country (CRM, ML job SP)
--   global_exec          global executives: aggregated pos_global layer only, all countries
--
-- Both group functions are checked: is_account_group_member() for account groups (prod, SCIM)
-- and is_member() for workspace-local groups (what Free Edition lets the demo create).

-- @for_each_country
CREATE OR REPLACE FUNCTION pos_${cc}.security.in_group(g STRING)
RETURNS BOOLEAN
COMMENT 'TRUE if the caller is in account group or workspace group g'
RETURN is_account_group_member(g) OR is_member(g);

CREATE OR REPLACE FUNCTION pos_${cc}.security.rf_country(country STRING)
RETURNS BOOLEAN
COMMENT 'Row filter: platform team, or the manager group of that row''s country'
RETURN pos_${cc}.security.in_group('data_platform')
    OR pos_${cc}.security.in_group(concat('mgr_', lower(country)));

CREATE OR REPLACE FUNCTION pos_${cc}.security.mask_pii(v STRING)
RETURNS STRING
COMMENT 'Column mask: raw value for data_platform / pii_reader_${cc}, else a stable pseudonym'
RETURN CASE
  WHEN v IS NULL THEN NULL
  WHEN pos_${cc}.security.in_group('data_platform') OR pos_${cc}.security.in_group('pii_reader_${cc}') THEN v
  ELSE concat('anon_', substr(sha2(v, 256), 1, 12))
END;

CREATE OR REPLACE FUNCTION pos_${cc}.security.mask_email(v STRING)
RETURNS STRING
COMMENT 'Column mask for email: raw for PII readers, else ****@domain'
RETURN CASE
  WHEN v IS NULL THEN NULL
  WHEN pos_${cc}.security.in_group('data_platform') OR pos_${cc}.security.in_group('pii_reader_${cc}') THEN v
  ELSE regexp_replace(v, '^[^@]+', '****')
END;
-- @end

CREATE OR REPLACE FUNCTION pos_global.security.in_group(g STRING)
RETURNS BOOLEAN
RETURN is_account_group_member(g) OR is_member(g);

CREATE OR REPLACE FUNCTION pos_global.security.rf_global(country STRING)
RETURNS BOOLEAN
COMMENT 'Row filter on the global layer: execs see every country, managers only their own'
RETURN pos_global.security.in_group('data_platform')
    OR pos_global.security.in_group('global_exec')
    OR pos_global.security.in_group(concat('mgr_', lower(country)));
