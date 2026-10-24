# Databricks notebook source
# MAGIC %md
# MAGIC # Train churn model for one country -> UC registry -> challenger -> validation gate -> champion
# MAGIC Trained per country on that country's data (residency), in each environment on that environment's data
# MAGIC ("deploy code, not models").

# COMMAND ----------
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("country", "ph")
ENV, CC = dbutils.widgets.get("env"), dbutils.widgets.get("country").lower()
CAT = f"pos_{CC}"
MODEL = f"{CAT}.{ENV}_ml.churn_model"
FEATURES_TABLE = f"{CAT}.{ENV}_ml.customer_features"

# COMMAND ----------
import json
import zlib

import mlflow
import numpy as np
import pandas as pd
from ml_lib.features import FEATURES, LABEL, bin_edges, proportions
from ml_lib.registry import champion_version, promote_if_better
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Shared/pos-ml-{ENV}-churn-{CC}")
client = MlflowClient()

snap = spark.sql(f"SELECT max(as_of_date) FROM {FEATURES_TABLE} WHERE {LABEL} IS NOT NULL").first()[0]
pdf = spark.table(FEATURES_TABLE).where(f"as_of_date = '{snap}' AND {LABEL} IS NOT NULL").toPandas()
# Holdout = a fixed 20% of CUSTOMERS (hash of loyalty_id), not a random row split: no model version
# ever trains on these customers, so champion and challenger are compared on data neither has seen.
SPLIT_POLICY = "customer_crc32_mod5_v1"
holdout = pdf["loyalty_id"].map(lambda s: zlib.crc32(s.encode()) % 5 == 0).to_numpy()
X, y = pdf[FEATURES].astype(float), pdf[LABEL].astype(int)
X_tr, X_te, y_tr, y_te = X[~holdout], X[holdout], y[~holdout], y[holdout]

# COMMAND ----------
params = {"max_iter": 250, "learning_rate": 0.06, "max_leaf_nodes": 31, "l2_regularization": 1.0}
with mlflow.start_run(run_name=f"churn-{CC}-{snap}") as run:
    model = HistGradientBoostingClassifier(**params, random_state=7).fit(X_tr, y_tr)
    p = model.predict_proba(X_te)[:, 1]
    auc, pr_auc = roc_auc_score(y_te, p), average_precision_score(y_te, p)
    mlflow.log_params({**params, "country": CC, "env": ENV, "train_rows": len(X_tr), "snapshot": str(snap)})
    mlflow.log_metrics({"val_auc": auc, "val_pr_auc": pr_auc, "churn_rate": float(y.mean())})
    mlflow.sklearn.log_model(model, name="model", pyfunc_predict_fn="predict_proba",
                             signature=infer_signature(X_te.head(50), model.predict_proba(X_te.head(50))),
                             input_example=X_te.head(5))
    mv = mlflow.register_model(f"runs:/{run.info.run_id}/model", MODEL)
    client.set_model_version_tag(MODEL, mv.version, "val_auc", f"{auc:.4f}")
    client.set_model_version_tag(MODEL, mv.version, "split_policy", SPLIT_POLICY)

# COMMAND ----------
# Validation gate: score the CURRENT champion on the same holdout, promote only if the challenger is not worse.
# If the champion was trained under a different split policy it may have seen this holdout, so its
# score here would be inflated; fall back to the AUC it recorded on its own holdout.
champ_auc, champ_metric_source = None, None
cv = champion_version(client, MODEL)
if cv:
    tags = client.get_model_version(MODEL, cv).tags
    if tags.get("split_policy") == SPLIT_POLICY:
        champ = mlflow.pyfunc.load_model(f"models:/{MODEL}@champion")
        champ_auc = float(roc_auc_score(y_te, np.asarray(champ.predict(X_te))[:, 1]))
        champ_metric_source = "same holdout"
    else:
        champ_auc, champ_metric_source = float(tags["val_auc"]), "champion's own holdout (split policy changed)"
g = promote_if_better(client, MODEL, mv.version, auc, champ_auc, higher_is_better=True, tolerance=0.01, floor=0.60)

# Drift baseline for this version: decile edges + proportions of training features and of scores.
rows = []
for f in FEATURES:
    edges = bin_edges(X_tr[f].to_numpy(float))
    rows.append((MODEL, str(mv.version), f, edges, proportions(X_tr[f].to_numpy(float), edges), int(len(X_tr))))
pe = bin_edges(p)
rows.append((MODEL, str(mv.version), "__prediction__", pe, proportions(p, pe), int(len(p))))
spark.createDataFrame(pd.DataFrame(rows, columns=["model_name", "model_version", "feature", "edges", "proportions", "n"])) \
     .write.mode("append").saveAsTable(f"{CAT}.{ENV}_ml.drift_baseline")

res = {"model": MODEL, "version": mv.version, "val_auc": round(auc, 4), "champion_auc": champ_auc,
       "champion_metric_source": champ_metric_source,
       "promoted": g.promoted, "reason": g.reason, "previous_champion": g.champion_version}
print(res)
dbutils.notebook.exit(json.dumps(res, default=str))
