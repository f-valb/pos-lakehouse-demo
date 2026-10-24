-- Unity Catalog layout. One catalog per country (= data-residency domain), one global catalog
-- for the aggregated, PII-free layer. Environments are schema prefixes inside each catalog.
--
-- PROD: each country catalog lives in its own metastore/workspace in that country's AWS
-- region and account (catalog storage = an S3 bucket in-country). The demo has one Free Edition
-- workspace, so all catalogs share one metastore and Databricks-managed default storage.

-- @for_each_country
CREATE CATALOG IF NOT EXISTS pos_${cc} COMMENT 'POS data domain ${CC}. Stored and processed in-country.';
ALTER CATALOG pos_${cc} SET TAGS ('data_country' = '${CC}', 'data_classification' = 'confidential', 'cost_center' = 'retail-data');
CREATE SCHEMA IF NOT EXISTS pos_${cc}.security COMMENT 'Row filter / column mask functions (env-independent)';
-- @end

CREATE CATALOG IF NOT EXISTS pos_global COMMENT 'Cross-region layer: aggregated, PII-free gold only.';
ALTER CATALOG pos_global SET TAGS ('data_country' = 'GLOBAL', 'data_classification' = 'internal', 'cost_center' = 'retail-data');
CREATE SCHEMA IF NOT EXISTS pos_global.security;

-- @for_each_env
-- @for_each_country
CREATE SCHEMA IF NOT EXISTS pos_${cc}.${env}_bronze COMMENT '${env}: raw, append-only, every pulled version';
CREATE SCHEMA IF NOT EXISTS pos_${cc}.${env}_silver COMMENT '${env}: translated, validated, deduplicated';
CREATE SCHEMA IF NOT EXISTS pos_${cc}.${env}_gold COMMENT '${env}: business aggregates for BI';
CREATE SCHEMA IF NOT EXISTS pos_${cc}.${env}_ml COMMENT '${env}: feature tables, models, predictions, monitoring';
CREATE VOLUME IF NOT EXISTS pos_${cc}.${env}_bronze.landing COMMENT 'Raw Parquet landing zone (prod: external volume on the country S3 bucket)';
-- @end
CREATE SCHEMA IF NOT EXISTS pos_global.${env}_gold COMMENT '${env}: global aggregated layer';
CREATE SCHEMA IF NOT EXISTS pos_global.${env}_ops COMMENT '${env}: FinOps / observability views';
-- @end
