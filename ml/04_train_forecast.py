# Databricks notebook source
# MAGIC %md
# MAGIC # Demand forecast, many-models pattern
# MAGIC One model per (store, category), fitted in parallel with `applyInPandas`, packaged as a single MLflow
# MAGIC pyfunc that routes rows to their model. Registered per country in UC, same champion/challenger gate.

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("country", "ph")
ENV, CC = dbutils.widgets.get("env"), dbutils.widgets.get("country").lower()
CAT = f"pos_{CC}"
MODEL = f"{CAT}.{ENV}_ml.demand_forecast"

# COMMAND ----------
import json
import os
import tempfile

import mlflow
from ml_lib import forecast
from ml_lib.registry import champion_version, promote_if_better
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Shared/pos-ml-{ENV}-forecast-{CC}")
client = MlflowClient()

daily = spark.sql(f"""
  SELECT store_id, category, business_date, CAST(sum(units) AS DOUBLE) AS units FROM (
    SELECT store_id, category, business_date, units FROM {CAT}.{ENV}_gold.sales_by_store_hour
    UNION ALL
    SELECT h.store_id, h.category, h.business_date, h.units FROM {CAT}.{ENV}_ml.store_category_daily_history h
    LEFT ANTI JOIN (SELECT DISTINCT store_id, business_date FROM {CAT}.{ENV}_gold.sales_by_store_hour) g
      ON g.store_id = h.store_id AND g.business_date = h.business_date)
  WHERE business_date < current_date()  -- today is incomplete; never train on a partial day
  GROUP BY 1, 2, 3""")
fits = daily.groupBy("store_id", "category").applyInPandas(forecast.fit_group, forecast.FIT_SCHEMA).toPandas()
fits.attrs = {}  # Spark Connect puts query metrics in attrs; pyarrow cannot serialize them to Parquet
wape = forecast.overall_wape(fits)

# COMMAND ----------
with mlflow.start_run(run_name=f"forecast-{CC}") as run:
    d = tempfile.mkdtemp()
    path = os.path.join(d, "params.parquet")
    fits.to_parquet(path)
    mlflow.log_metrics({"backtest_wape": wape, "groups": len(fits)})
    mlflow.log_params({"country": CC, "env": ENV, "train_days": forecast.TRAIN_DAYS, "backtest_days": forecast.BACKTEST_DAYS})
    mlflow.log_table(fits[["store_id", "category", "backtest_wape", "n_obs"]], "per_group_backtest.json")
    import pandas as pd

    example = pd.DataFrame({"store_id": [fits.store_id[0]], "category": [fits.category[0]], "business_date": ["2026-11-01"]})
    mlflow.pyfunc.log_model(name="model", python_model=forecast.ManyModelsForecaster(), artifacts={"params": path},
                            code_paths=[os.path.dirname(forecast.__file__)], input_example=example)
    mv = mlflow.register_model(f"runs:/{run.info.run_id}/model", MODEL)
    client.set_model_version_tag(MODEL, mv.version, "backtest_wape", f"{wape:.4f}")

champ_wape = None
cv = champion_version(client, MODEL)
if cv:
    champ_wape = float(client.get_model_version(MODEL, cv).tags.get("backtest_wape", "nan"))
g = promote_if_better(client, MODEL, mv.version, wape, champ_wape, higher_is_better=False, tolerance=0.02, floor=0.5)
spark.createDataFrame(fits.assign(model_version=str(mv.version))).write.mode("overwrite") \
     .saveAsTable(f"{CAT}.{ENV}_ml.forecast_model_params")

res = {"model": MODEL, "version": mv.version, "groups": len(fits), "backtest_wape": round(wape, 4),
       "champion_wape": champ_wape, "promoted": g.promoted, "reason": g.reason}
print(res)
dbutils.notebook.exit(json.dumps(res, default=str))
