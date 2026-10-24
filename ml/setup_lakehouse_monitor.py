"""Create Lakehouse Monitoring (data profiling) monitors next to the custom PSI monitor:

* inference-log monitor on <env>_ml.churn_inference_log (prediction drift per model version)
* snapshot monitor on <env>_ml.customer_features (feature profile over time)

Profile + drift metric tables land in <env>_ml; refreshed after each scoring run (or on its own
schedule). The custom 06_monitor.py job stays as the alerting/retrain trigger because it compares
against the champion's *training* baseline and drives the job condition task.

    uv run python ml/setup_lakehouse_monitor.py --country ph --env dev
"""

import argparse

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists
from databricks.sdk.service.catalog import (
    MonitorInferenceLog,
    MonitorInferenceLogProblemType,
    MonitorSnapshot,
)

ap = argparse.ArgumentParser()
ap.add_argument("--country", default="ph")
ap.add_argument("--env", default="dev")
a = ap.parse_args()
w = WorkspaceClient()
schema = f"pos_{a.country}.{a.env}_ml"
assets = f"/Workspace/Shared/pos-ml-monitoring/{a.env}/{a.country}"
w.workspace.mkdirs(assets)

monitors = {
    f"{schema}.churn_inference_log": dict(inference_log=MonitorInferenceLog(
        timestamp_col="scored_at", granularities=["1 day"], model_id_col="model_version",
        prediction_col="churn_score", problem_type=MonitorInferenceLogProblemType.PROBLEM_TYPE_REGRESSION)),
    f"{schema}.customer_features": dict(snapshot=MonitorSnapshot()),
}
for table, kind in monitors.items():
    try:
        m = w.quality_monitors.create(table_name=table, assets_dir=assets, output_schema_name=schema, **kind)
        print(f"created monitor on {table}: status={m.status}, dashboard={m.dashboard_id}")
    except ResourceAlreadyExists:
        print(f"monitor exists on {table}")
    status = w.quality_monitors.get(table_name=table).status
    if str(status).endswith("PENDING"):
        print(f"  {status}: initial refresh runs automatically")
    else:
        r = w.quality_monitors.run_refresh(table_name=table)
        print(f"  refresh {r.refresh_id} {r.state}")
