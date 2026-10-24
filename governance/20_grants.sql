-- Grants (least privilege). Row filters / masks do the fine-grained part; grants decide which
-- layers a group can touch at all. In prod, groups are Entra ID groups synced by SCIM.
--
--   group            pos_<cc> (country)                        pos_global
--   data_platform    ALL PRIVILEGES                            ALL PRIVILEGES
--   mgr_<cc>         gold only (own country), PII masked       gold, row filter = own country
--   pii_reader_<cc>  silver + gold (own country), PII raw      -
--   global_exec      nothing                                    gold, all countries (aggregates)

-- @for_each_country
GRANT ALL PRIVILEGES ON CATALOG pos_${cc} TO `data_platform`;
GRANT USE CATALOG ON CATALOG pos_${cc} TO `mgr_${cc}`;
GRANT USE CATALOG ON CATALOG pos_${cc} TO `pii_reader_${cc}`;
ALTER CATALOG pos_${cc} ENABLE PREDICTIVE OPTIMIZATION;
-- @end
GRANT ALL PRIVILEGES ON CATALOG pos_global TO `data_platform`;
GRANT USE CATALOG ON CATALOG pos_global TO `global_exec`;
ALTER CATALOG pos_global ENABLE PREDICTIVE OPTIMIZATION;

-- @for_each_env
-- @for_each_country
GRANT USE SCHEMA, SELECT ON SCHEMA pos_${cc}.${env}_gold TO `mgr_${cc}`;
GRANT USE SCHEMA, SELECT ON SCHEMA pos_${cc}.${env}_gold TO `pii_reader_${cc}`;
GRANT USE SCHEMA, SELECT ON SCHEMA pos_${cc}.${env}_silver TO `pii_reader_${cc}`;
-- @end
GRANT USE SCHEMA, SELECT ON SCHEMA pos_global.${env}_gold TO `global_exec`;
-- @end

-- Country managers also read the global layer (their row filter limits them to their country).
-- @for_each_country
GRANT USE CATALOG ON CATALOG pos_global TO `mgr_${cc}`;
GRANT USE SCHEMA, SELECT ON SCHEMA pos_global.dev_gold TO `mgr_${cc}`;
GRANT USE SCHEMA, SELECT ON SCHEMA pos_global.staging_gold TO `mgr_${cc}`;
GRANT USE SCHEMA, SELECT ON SCHEMA pos_global.prod_gold TO `mgr_${cc}`;
-- @end
