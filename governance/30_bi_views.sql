-- BI views in the global layer: PII-free aggregates of the per-country ML outputs, so the
-- one global dashboard / Power BI model never reads a country catalog directly.
-- @for_each_env
CREATE OR REPLACE VIEW pos_global.${env}_gold.bi_forecast_vs_actual
COMMENT 'Daily units per country: actuals (gold; before the live stream started: SIMULATED history) and the 14-day forecast (ML). Aggregated, no store detail'
AS
WITH live AS (
  SELECT country, business_date, units FROM pos_ph.${env}_gold.sales_by_store_hour
  UNION ALL SELECT country, business_date, units FROM pos_sg.${env}_gold.sales_by_store_hour
  UNION ALL SELECT country, business_date, units FROM pos_de.${env}_gold.sales_by_store_hour
  UNION ALL SELECT country, business_date, units FROM pos_us.${env}_gold.sales_by_store_hour),
hist AS (
  SELECT country, business_date, units FROM pos_ph.${env}_ml.store_category_daily_history
  UNION ALL SELECT country, business_date, units FROM pos_sg.${env}_ml.store_category_daily_history
  UNION ALL SELECT country, business_date, units FROM pos_de.${env}_ml.store_category_daily_history
  UNION ALL SELECT country, business_date, units FROM pos_us.${env}_ml.store_category_daily_history)
SELECT country, business_date, 'actual' AS series, sum(units) AS units FROM live GROUP BY ALL
UNION ALL
SELECT h.country, h.business_date, 'actual (simulated history)' AS series, sum(h.units) AS units
FROM hist h WHERE h.business_date < (SELECT min(business_date) FROM live l WHERE l.country = h.country)
GROUP BY ALL
UNION ALL
SELECT country, business_date, 'forecast' AS series, round(sum(forecast_units)) AS units FROM (
  SELECT country, business_date, forecast_units FROM pos_ph.${env}_gold.demand_forecast
  UNION ALL SELECT country, business_date, forecast_units FROM pos_sg.${env}_gold.demand_forecast
  UNION ALL SELECT country, business_date, forecast_units FROM pos_de.${env}_gold.demand_forecast
  UNION ALL SELECT country, business_date, forecast_units FROM pos_us.${env}_gold.demand_forecast)
GROUP BY ALL;

CREATE OR REPLACE VIEW pos_global.${env}_gold.bi_churn_risk
COMMENT 'Churn risk distribution per country (counts only, no customer identifiers)'
AS
SELECT country, as_of_date, risk_band, model_version, count(*) AS customers, round(avg(churn_score), 4) AS avg_score
FROM (
  SELECT * FROM pos_ph.${env}_gold.churn_predictions
  UNION ALL SELECT * FROM pos_sg.${env}_gold.churn_predictions
  UNION ALL SELECT * FROM pos_de.${env}_gold.churn_predictions
  UNION ALL SELECT * FROM pos_us.${env}_gold.churn_predictions)
WHERE as_of_date = (SELECT max(as_of_date) FROM pos_ph.${env}_gold.churn_predictions)
GROUP BY ALL;
-- @end
