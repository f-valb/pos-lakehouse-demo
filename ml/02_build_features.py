# Databricks notebook source
# MAGIC %md
# MAGIC # Churn feature table (Unity Catalog)
# MAGIC `pos_<cc>.<env>_ml.customer_features`: primary key `(loyalty_id, as_of_date TIMESERIES)`, which makes it a
# MAGIC UC feature table (usable with `FeatureLookup` / point-in-time joins). One snapshot per `as_of_date`:
# MAGIC * `mode=train`: as_of = yesterday - 30 days, with the `churned_30d` label (did they buy in the next 30 days?)
# MAGIC * `mode=score`: as_of = yesterday (last complete business day), no label
# MAGIC
# MAGIC `inject_drift=<cc>` (score mode only) distorts today's snapshot for that country, for the drift demo.

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("country", "ph")
dbutils.widgets.dropdown("mode", "score", ["train", "score"])
dbutils.widgets.text("inject_drift", "")
ENV, CC, MODE = dbutils.widgets.get("env"), dbutils.widgets.get("country").lower(), dbutils.widgets.get("mode")
DRIFT = CC in [c.strip().lower() for c in dbutils.widgets.get("inject_drift").split(",")]
CAT = f"pos_{CC}"
TABLE = f"{CAT}.{ENV}_ml.customer_features"

# COMMAND ----------
import json
from datetime import date, timedelta

from ml_lib.features import FEATURES, LABEL, feature_sql

# Snapshots are taken at the end of a COMPLETE business day; a partial "today" would shift recency
# for everyone and look like drift.
last_complete_day = date.today() - timedelta(days=1)
as_of = last_complete_day - timedelta(days=30) if MODE == "train" else last_complete_day

# Live gold activity + simulated history, same schema. Live wins on overlapping days.
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW daily AS
SELECT country, loyalty_id, business_date, transactions, CAST(spend_usd AS DOUBLE) spend_usd,
       categories, card_share, ewallet_share
FROM {CAT}.{ENV}_gold.customer_daily
UNION ALL
SELECT h.country, h.loyalty_id, h.business_date, h.transactions, h.spend_usd, h.categories, h.card_share, h.ewallet_share
FROM {CAT}.{ENV}_ml.customer_daily_history h
LEFT ANTI JOIN {CAT}.{ENV}_gold.customer_daily g ON g.loyalty_id = h.loyalty_id AND g.business_date = h.business_date
""")
feats = spark.sql(feature_sql("daily", as_of.isoformat(), with_label=MODE == "train"))
if MODE == "score":
    from pyspark.sql import functions as F

    feats = feats.withColumn(LABEL, F.lit(None).cast("int"))
    if DRIFT:
        # Simulated upstream change: a currency/scale bug triples spend and tender mapping flips.
        feats = (feats.withColumn("spend_30_usd", F.col("spend_30_usd") * 3)
                      .withColumn("spend_90_usd", F.col("spend_90_usd") * 3)
                      .withColumn("avg_basket_90_usd", F.col("avg_basket_90_usd") * 3)
                      .withColumn("card_share_90", 1 - F.col("card_share_90")))

cols = ", ".join(f"{f} DOUBLE" for f in FEATURES)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  loyalty_id STRING NOT NULL, as_of_date DATE NOT NULL, country STRING, {cols}, {LABEL} INT,
  CONSTRAINT customer_features_pk PRIMARY KEY (loyalty_id, as_of_date TIMESERIES)
) CLUSTER BY (as_of_date)
COMMENT 'Churn features per loyalty member and snapshot date (UC feature table)'
""")
spark.sql(f"ALTER TABLE {TABLE} ALTER COLUMN loyalty_id SET MASK {CAT}.security.mask_pii")
if MODE == "score":  # scoring/monitoring read the newest unlabelled snapshot; never leave one from the future
    spark.sql(f"DELETE FROM {TABLE} WHERE {LABEL} IS NULL AND as_of_date > DATE'{as_of}'")
(feats.selectExpr("loyalty_id", "as_of_date", "country", *[f"CAST({f} AS DOUBLE) AS {f}" for f in FEATURES], LABEL)
      .write.mode("overwrite").option("replaceWhere", f"as_of_date = '{as_of}'").saveAsTable(TABLE))

n = spark.table(TABLE).where(f"as_of_date = '{as_of}'").count()
res = {"table": TABLE, "as_of": as_of.isoformat(), "mode": MODE, "rows": n, "drift_injected": DRIFT}
if MODE == "train":
    res["churn_rate"] = spark.table(TABLE).where(f"as_of_date = '{as_of}'").agg({LABEL: "avg"}).first()[0]
print(res)
dbutils.notebook.exit(json.dumps(res))
