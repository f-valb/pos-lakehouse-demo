# Databricks notebook source
# MAGIC %md
# MAGIC # Drift + data-quality monitor (custom; see README for Lakehouse Monitoring)
# MAGIC For each country: PSI of every feature and of the score distribution vs the champion's training
# MAGIC baseline, plus data-quality checks (null rates, volume). Results go to `<env>_ml.monitoring_metrics`.
# MAGIC Sets task values `drift_detected` / `drift_countries` so the job can trigger a retrain, and a
# MAGIC DBSQL alert on the metrics table emails the team.

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("countries", "ph,sg,de,us")
ENV = dbutils.widgets.get("env")
COUNTRIES = [c.strip().lower() for c in dbutils.widgets.get("countries").split(",") if c.strip()]

# COMMAND ----------
import json
from datetime import UTC, datetime

import mlflow
import numpy as np
import pandas as pd
from ml_lib.features import FEATURES, PSI_ALERT, TIME_TRENDING, proportions, psi
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
run_ts = datetime.now(UTC)
rows, drift_countries = [], []


def add(ctx, kind, metric, value, threshold, is_breach):
    rows.append((run_ts, *ctx, kind, metric, float(value), float(threshold), bool(is_breach)))


for cc in COUNTRIES:
    cat = f"pos_{cc}"
    name = f"{cat}.{ENV}_ml.churn_model"
    version = client.get_model_version_by_alias(name, "champion").version
    base = spark.table(f"{cat}.{ENV}_ml.drift_baseline").where(f"model_name = '{name}' AND model_version = '{version}'").toPandas()
    if base.empty:
        continue
    base = base.drop_duplicates("feature", keep="last").set_index("feature")
    snap = spark.sql(f"SELECT max(as_of_date) FROM {cat}.{ENV}_ml.customer_features WHERE churned_30d IS NULL").first()[0]
    cur = spark.table(f"{cat}.{ENV}_ml.customer_features").where(f"as_of_date = '{snap}'").toPandas()
    preds = spark.sql(f"""SELECT churn_score FROM {cat}.{ENV}_ml.churn_inference_log
                          WHERE scored_at = (SELECT max(scored_at) FROM {cat}.{ENV}_ml.churn_inference_log)""").toPandas()
    breached = False
    ctx = (cc.upper(), name, str(version), str(snap))

    for f in FEATURES:
        v = psi(base.loc[f, "proportions"], proportions(cur[f].to_numpy(float), list(base.loc[f, "edges"])))
        alerting = f not in TIME_TRENDING
        add(ctx, "feature_psi" if alerting else "feature_psi_info", f, v, PSI_ALERT, alerting and v > PSI_ALERT)
        breached |= alerting and v > PSI_ALERT
        null_rate = float(cur[f].isna().mean())
        add(ctx, "dq_null_rate", f, null_rate, 0.05, null_rate > 0.05)
    if not preds.empty:
        v = psi(base.loc["__prediction__", "proportions"],
                proportions(preds["churn_score"].to_numpy(float), list(base.loc["__prediction__", "edges"])))
        add(ctx, "prediction_psi", "churn_score", v, PSI_ALERT, v > PSI_ALERT)
        breached |= v > PSI_ALERT
    ratio = len(cur) / max(int(base.loc[FEATURES[0], "n"]) / 0.8, 1)  # baseline n = 80% train split
    add(ctx, "dq_volume_ratio", "rows_vs_baseline", ratio, 0.5, ratio < 0.5 or ratio > 2.0)
    if breached:
        drift_countries.append(cc)

df = pd.DataFrame(rows, columns=["run_ts", "country", "model_name", "model_version", "as_of_date", "metric_type",
                                 "metric", "value", "threshold", "breached"])
spark.createDataFrame(df).write.mode("append").saveAsTable(f"pos_global.{ENV}_ops.ml_monitoring_metrics")

top = df[df.metric_type.str.endswith("psi")].sort_values("value", ascending=False).head(8)
print(top[["country", "metric", "value", "breached"]].to_string(index=False))
dbutils.jobs.taskValues.set("drift_detected", "true" if drift_countries else "false")
dbutils.jobs.taskValues.set("drift_countries", json.dumps(drift_countries))
dbutils.notebook.exit(json.dumps({"drift_countries": drift_countries, "metrics": len(df),
                                  "max_psi": float(np.nanmax(df[df.metric_type.str.endswith('psi')].value))}))
