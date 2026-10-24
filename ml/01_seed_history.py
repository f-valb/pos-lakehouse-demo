# Databricks notebook source
# MAGIC %md
# MAGIC # Seed SIMULATED ML history
# MAGIC The live stream only covers hours, so churn labels and weekly seasonality need history. This writes
# MAGIC 180 days of synthetic per-customer and per-store activity, consistent with the live generator.
# MAGIC In production this notebook does not exist; the history is the real silver/gold data.

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("countries", "ph,sg,de,us")
dbutils.widgets.text("records_per_minute", "50000")  # must match POS_API_RECORDS_PER_MINUTE
ENV = dbutils.widgets.get("env")
COUNTRIES = [c.strip().lower() for c in dbutils.widgets.get("countries").split(",") if c.strip()]

# COMMAND ----------
import json
import os
import sys
from datetime import date

sys.path.append(os.path.abspath(".."))  # repo root, for common/customers.py (shared with the live generator)
from ml_lib import history

today = date.today()
out = {}
for cc in COUNTRIES:
    cust = spark.createDataFrame(history.customer_daily_history(cc.upper(), today))
    cust.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"pos_{cc}.{ENV}_ml.customer_daily_history")
    stores = spark.createDataFrame(history.store_category_daily_history(
        cc.upper(), today, int(dbutils.widgets.get("records_per_minute"))))
    stores.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"pos_{cc}.{ENV}_ml.store_category_daily_history")
    spark.sql(f"ALTER TABLE pos_{cc}.{ENV}_ml.customer_daily_history ALTER COLUMN loyalty_id SET MASK pos_{cc}.security.mask_pii")
    spark.sql(f"COMMENT ON TABLE pos_{cc}.{ENV}_ml.customer_daily_history IS 'SIMULATED 180-day history for ML (demo only)'")
    out[cc] = {"customer_days": cust.count(), "store_category_days": stores.count()}
print(out)
dbutils.notebook.exit(json.dumps(out))
