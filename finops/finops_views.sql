-- FinOps views (platform-wide, not per environment): pos_global.finops.
-- Spend is attributed by TAGS: env / component / data_country / cost_center come from the bundle
-- presets + resource tags (jobs, pipelines) and, in prod, from serverless budget policies.

CREATE SCHEMA IF NOT EXISTS pos_global.finops COMMENT 'Cost attribution views over system tables';

-- 1) REAL billed usage (what prod uses). Free Edition does not emit billable usage, so this view
--    is empty in the demo workspace; the dashboard shows it next to the runtime estimate below.
CREATE OR REPLACE VIEW pos_global.finops.billing_daily
COMMENT 'Daily list-price cost by env / component / country / product from system.billing.usage'
AS
WITH prices AS (
  SELECT sku_name, price_start_time, price_end_time, pricing.effective_list.default AS usd_per_unit
  FROM system.billing.list_prices
),
latest_pipelines AS (
  SELECT pipeline_id, name FROM system.lakeflow.pipelines
  QUALIFY row_number() OVER (PARTITION BY pipeline_id ORDER BY change_time DESC) = 1
),
latest_jobs AS (
  SELECT job_id, name FROM system.lakeflow.jobs
  QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
)
SELECT
  u.usage_date,
  coalesce(u.custom_tags['env'], 'untagged') AS env,
  coalesce(u.custom_tags['component'], 'untagged') AS component,
  coalesce(u.custom_tags['data_country'], 'untagged') AS data_country,
  coalesce(u.custom_tags['cost_center'], 'untagged') AS cost_center,
  u.billing_origin_product AS product,
  coalesce(lp.name, lj.name, u.usage_metadata.warehouse_id, 'other') AS workload,
  sum(u.usage_quantity) AS dbus,
  sum(u.usage_quantity * p.usd_per_unit) AS list_cost_usd
FROM system.billing.usage u
LEFT JOIN prices p
  ON u.sku_name = p.sku_name AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
LEFT JOIN latest_pipelines lp ON u.usage_metadata.dlt_pipeline_id = lp.pipeline_id
LEFT JOIN latest_jobs lj ON u.usage_metadata.job_id = lj.job_id
GROUP BY ALL;

-- 2) RUNTIME-based ESTIMATE (works on Free Edition). Serverless compute-minutes per tagged
--    pipeline update / job task, priced at an assumed blended rate. Clearly an estimate: real
--    serverless cost depends on autoscaling, Photon and the SKU.
CREATE OR REPLACE VIEW pos_global.finops.runtime_daily
COMMENT 'ESTIMATE: compute minutes x assumed USD 0.10/min, by env / component / country (from system.lakeflow)'
AS
WITH p AS (
  SELECT pipeline_id, name, tags FROM system.lakeflow.pipelines
  QUALIFY row_number() OVER (PARTITION BY pipeline_id ORDER BY change_time DESC) = 1
),
j AS (
  SELECT job_id, name, tags FROM system.lakeflow.jobs
  QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
),
pipeline_runs AS (
  SELECT date(t.period_start_time) AS usage_date, p.tags AS tags, p.name AS workload, 'PIPELINE' AS kind,
         timestampdiff(SECOND, t.period_start_time, t.period_end_time) / 60.0 AS minutes
  FROM system.lakeflow.pipeline_update_timeline t JOIN p USING (pipeline_id)
),
job_runs AS ( -- medallion job tasks are pipeline tasks, already counted above
  SELECT date(t.period_start_time) AS usage_date, j.tags AS tags, j.name AS workload, 'JOB' AS kind,
         timestampdiff(SECOND, t.period_start_time, t.period_end_time) / 60.0 AS minutes
  FROM system.lakeflow.job_task_run_timeline t JOIN j USING (job_id)
  WHERE coalesce(j.tags['component'], '') <> 'medallion'
)
SELECT usage_date,
       coalesce(tags['env'], 'untagged') AS env,
       coalesce(tags['component'], 'untagged') AS component,
       coalesce(tags['data_country'], 'multi') AS data_country,
       coalesce(tags['cost_center'], 'untagged') AS cost_center,
       kind, workload,
       round(sum(minutes), 1) AS compute_minutes,
       round(sum(minutes) * 0.10, 2) AS est_cost_usd
FROM (SELECT * FROM pipeline_runs UNION ALL SELECT * FROM job_runs)
GROUP BY ALL;

-- 3) Tag hygiene: share of compute that is untagged (target: 0%).
CREATE OR REPLACE VIEW pos_global.finops.tag_coverage AS
SELECT usage_date, round(100 * sum(CASE WHEN env = 'untagged' OR cost_center = 'untagged' THEN compute_minutes ELSE 0 END)
                         / greatest(sum(compute_minutes), 1), 1) AS untagged_pct
FROM pos_global.finops.runtime_daily GROUP BY usage_date;

GRANT USE SCHEMA, SELECT ON SCHEMA pos_global.finops TO `data_platform`;
