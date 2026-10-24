#!/usr/bin/env bash
# Tear down everything the demo created. Destructive: asks before each part unless --yes.
#   ./scripts/teardown.sh          # interactive
#   ./scripts/teardown.sh --yes    # no prompts
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"
YES=${1:-}

confirm() { [[ "$YES" == "--yes" ]] && return 0; read -r -p "$1 [y/N] " a; [[ "$a" == "y" || "$a" == "Y" ]]; }

if confirm "Stop local processes (mock API, poller) and remove LocalStack + its volume?"; then
  pkill -f "ingestion.run_local" || true
  pkill -f "uvicorn mock_pos_api.app" || true
  docker compose down -v
fi

if confirm "Destroy the bundle (jobs, pipelines, dashboards, alerts) in dev, staging and prod?"; then
  for t in dev staging prod; do databricks bundle destroy -t "$t" --auto-approve || true; done
fi

if confirm "DROP catalogs pos_ph, pos_sg, pos_de, pos_us, pos_global (CASCADE: all tables, models, volumes)?"; then
  for c in pos_ph pos_sg pos_de pos_us pos_global; do
    uv run python scripts/dbsql.py -e "DROP CATALOG IF EXISTS $c CASCADE" || true
  done
fi

if confirm "Delete MLflow experiments /Shared/pos-ml-* and monitoring assets?"; then
  databricks workspace list /Shared -o json 2>/dev/null | python3 -c '
import json, sys
for o in json.load(sys.stdin):
    if o["path"].split("/")[-1].startswith("pos-ml"):
        print(o["path"])' | while read -r p; do databricks workspace delete --recursive "$p" || true; done
fi

if confirm "Delete the demo account groups and test service principals?"; then
  uv run python - <<'EOF'
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
names = ["data_platform", "global_exec"] + [f"{p}_{c}" for p in ("mgr", "pii_reader") for c in ("ph", "sg", "de", "us")]
for kind, items in (("Groups", names), ("ServicePrincipals", ["sp-demo-mgr-ph", "sp-demo-global-exec"])):
    for n in items:
        res = w.api_client.do("GET", f"/api/2.0/account/scim/v2/{kind}", query={"filter": f'displayName eq "{n}"'})
        for r in res.get("Resources") or []:
            w.api_client.do("DELETE", f"/api/2.0/account/scim/v2/{kind}/{r['id']}")
            print("deleted", kind, n)
EOF
fi
echo "teardown finished"
