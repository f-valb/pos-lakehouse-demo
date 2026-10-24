# Databricks notebook source
# MAGIC %md
# MAGIC # Roll back a model by moving the `@champion` alias
# MAGIC No retraining, no redeploy: batch scoring resolves `models:/<name>@champion` on its next run.

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("country", "ph")
dbutils.widgets.dropdown("model", "churn_model", ["churn_model", "demand_forecast"])
ENV, CC, M = dbutils.widgets.get("env"), dbutils.widgets.get("country").lower(), dbutils.widgets.get("model")

# COMMAND ----------
import json

import mlflow
from ml_lib.registry import rollback
from mlflow import MlflowClient

mlflow.set_registry_uri("databricks-uc")
name = f"pos_{CC}.{ENV}_ml.{M}"
old, new = rollback(MlflowClient(), name)
res = {"model": name, "rolled_back_from": old, "champion_now": new}
print(res)
dbutils.notebook.exit(json.dumps(res))
