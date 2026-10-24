"""Print versions, aliases and gate decisions of a UC-registered model.

    MLFLOW_TRACKING_URI=databricks uv run python scripts/model_versions.py pos_ph.dev_ml.churn_model
"""

import sys

import mlflow
from mlflow import MlflowClient

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
c = MlflowClient()
name = sys.argv[1]
champ = c.get_model_version_by_alias(name, "champion")
print(f"{name}: @champion = v{champ.version} (previous_champion tag: {champ.tags.get('previous_champion')})")
for v in sorted(int(m.version) for m in c.search_model_versions(f"name='{name}'")):
    mv = c.get_model_version(name, str(v))  # search results carry no tags in UC
    print(f"  v{mv.version:<3} aliases={mv.aliases!s:28} status={mv.tags.get('validation_status', '-'):7} "
          f"metric={mv.tags.get('val_auc') or mv.tags.get('backtest_wape') or '-':7} {mv.tags.get('validation_reason', '')[:70]}"
          f"{' [rolled back]' if mv.tags.get('rolled_back') else ''}")
