#!/usr/bin/env bash
# One-time CI setup, run by a workspace admin (you), NOT by CI:
#   1. lets the CI service principal sp-pos-cicd deploy: workspace access, data_platform group,
#      CAN_USE on the SQL warehouse
#   2. creates an OAuth secret for it and stores it as the GitHub Actions secret
#      DATABRICKS_CLIENT_SECRET. The secret is piped straight into `gh secret set`; it is never
#      printed or written to disk.
#
#   ./scripts/setup_ci.sh f-valb/pos-lakehouse-demo
#
# PROD: skip the secret entirely and use GitHub OIDC workload identity federation (an
# account-level federation policy on the SP trusting token.actions.githubusercontent.com).
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"
REPO=${1:?usage: setup_ci.sh <owner/repo>}

SP_JSON=$(uv run python - <<'EOF'
import json
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

w = WorkspaceClient()
api = w.api_client.do
patch = "urn:ietf:params:scim:api:messages:2.0:PatchOp"


def find(kind, name):
    r = api("GET", f"/api/2.0/account/scim/v2/{kind}", query={"filter": f'displayName eq "{name}"'})
    return (r.get("Resources") or [None])[0]


sp = find("ServicePrincipals", "sp-pos-cicd") or api("POST", "/api/2.0/account/scim/v2/ServicePrincipals", body={
    "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"], "displayName": "sp-pos-cicd", "active": True})
api("PUT", f"/api/2.0/preview/permissionassignments/principals/{sp['id']}", body={"permissions": ["USER"]})
# Workspace assignment alone grants no entitlements: the bundle CLI needs workspace access, and
# the integration tests need Databricks SQL access. Retry: a fresh assignment takes a moment to appear.
for attempt in range(6):
    try:
        api("PATCH", f"/api/2.0/preview/scim/v2/ServicePrincipals/{sp['id']}", body={"schemas": [patch], "Operations": [
            {"op": "add", "path": "entitlements", "value": [{"value": "workspace-access"}, {"value": "databricks-sql-access"}]}]})
        break
    except Exception:
        if attempt == 5:
            raise
        time.sleep(3 * (attempt + 1))
dp = find("Groups", "data_platform")
api("PATCH", f"/api/2.0/account/scim/v2/Groups/{dp['id']}", body={
    "schemas": [patch], "Operations": [{"op": "add", "path": "members", "value": [{"value": sp["id"]}]}]})
wh = next(iter(w.warehouses.list())).id
w.permissions.update("warehouses", wh, access_control_list=[
    AccessControlRequest(service_principal_name=sp["applicationId"], permission_level=PermissionLevel.CAN_USE)])
print(json.dumps({"id": sp["id"], "application_id": sp["applicationId"], "host": w.config.host}))
EOF
)
SP_ID=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["id"])' "$SP_JSON")
APP_ID=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["application_id"])' "$SP_JSON")
HOST=$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["host"])' "$SP_JSON")
echo "sp-pos-cicd ($APP_ID): workspace + SQL entitlements, data_platform, warehouse CAN_USE"

gh variable set DATABRICKS_HOST --repo "$REPO" --body "$HOST"
gh variable set DATABRICKS_CLIENT_ID --repo "$REPO" --body "$APP_ID"
databricks api post "/api/2.0/accounts/servicePrincipals/$SP_ID/credentials/secrets" --json '{}' \
  | python3 -c 'import json,sys; sys.stdout.write(json.load(sys.stdin)["secret"])' \
  | gh secret set DATABRICKS_CLIENT_SECRET --repo "$REPO"
echo "GitHub: variables DATABRICKS_HOST, DATABRICKS_CLIENT_ID and secret DATABRICKS_CLIENT_SECRET set on $REPO"
