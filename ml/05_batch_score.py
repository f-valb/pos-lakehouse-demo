# Databricks notebook source
# MAGIC %md
# MAGIC # Batch scoring (no real-time endpoint)
# MAGIC `mlflow.pyfunc.spark_udf` over the `@champion` alias:
# MAGIC * churn scores for today's feature snapshot -> `pos_<cc>.<env>_gold.churn_predictions` (+ inference log)
# MAGIC * 14-day demand forecast per store x category -> `pos_<cc>.<env>_gold.demand_forecast`

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("country", "ph")
ENV, CC = dbutils.widgets.get("env"), dbutils.widgets.get("country").lower()
CAT = f"pos_{CC}"

# COMMAND ----------
import json

import mlflow
from ml_lib.features import FEATURES
from mlflow import MlflowClient
from pyspark.sql import functions as F

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

churn_name = f"{CAT}.{ENV}_ml.churn_model"
churn_v = client.get_model_version_by_alias(churn_name, "champion").version
churn_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{churn_name}@champion", result_type="array<double>", env_manager="local")
snap = spark.sql(f"SELECT max(as_of_date) FROM {CAT}.{ENV}_ml.customer_features WHERE churned_30d IS NULL").first()[0]
feats = spark.table(f"{CAT}.{ENV}_ml.customer_features").where(f"as_of_date = '{snap}'")
scored = (feats.withColumn("churn_score", churn_udf(F.struct(*[F.col(f) for f in FEATURES]))[1])
               .withColumn("risk_band", F.when(F.col("churn_score") >= 0.6, "HIGH")
                                         .when(F.col("churn_score") >= 0.3, "MEDIUM").otherwise("LOW"))
               .withColumn("model_version", F.lit(churn_v))
               .withColumn("scored_at", F.current_timestamp()))

pred_table = f"{CAT}.{ENV}_gold.churn_predictions"
(scored.select("loyalty_id", "as_of_date", "country", "churn_score", "risk_band", "model_version", "scored_at")
       .write.mode("overwrite").option("replaceWhere", f"as_of_date = '{snap}'").saveAsTable(pred_table))
spark.sql(f"ALTER TABLE {pred_table} ALTER COLUMN loyalty_id SET MASK {CAT}.security.mask_pii")
# Inference log = features + prediction, the input for drift / Lakehouse Monitoring.
scored.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{CAT}.{ENV}_ml.churn_inference_log")
spark.sql(f"ALTER TABLE {CAT}.{ENV}_ml.churn_inference_log ALTER COLUMN loyalty_id SET MASK {CAT}.security.mask_pii")

# COMMAND ----------
fc_name = f"{CAT}.{ENV}_ml.demand_forecast"
fc_v = client.get_model_version_by_alias(fc_name, "champion").version
fc_udf = mlflow.pyfunc.spark_udf(spark, f"models:/{fc_name}@champion", result_type="double", env_manager="local")
grid = (spark.table(f"{CAT}.{ENV}_ml.forecast_model_params").select("store_id", "category").distinct()
        .crossJoin(spark.sql("SELECT explode(sequence(current_date(), date_add(current_date(), 13))) AS d"))
        .withColumn("business_date", F.date_format("d", "yyyy-MM-dd")))
fc = (grid.withColumn("forecast_units", fc_udf(F.struct("store_id", "category", "business_date")))
          .select("store_id", "category", F.to_date("business_date").alias("business_date"), "forecast_units",
                  F.lit(CC.upper()).alias("country"), F.lit(fc_v).alias("model_version"),
                  F.current_timestamp().alias("generated_at")))
fc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CAT}.{ENV}_gold.demand_forecast")

res = {"churn_version": churn_v, "as_of": str(snap), "scored": spark.table(pred_table).where(f"as_of_date = '{snap}'").count(),
       "forecast_version": fc_v, "forecast_rows": spark.table(f"{CAT}.{ENV}_gold.demand_forecast").count()}
print(res)
dbutils.notebook.exit(json.dumps(res))
