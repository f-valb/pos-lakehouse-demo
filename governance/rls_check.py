"""Acceptance check for row-level security, run as two real identities.

  sp-demo-mgr-ph       member of mgr_ph       (stands in for a PH country manager)
  sp-demo-global-exec  member of global_exec  (stands in for a global executive)

For each identity an OAuth secret is created, used in memory for the queries, and deleted again.
Nothing is written to disk. In prod these would be Entra ID users synced by SCIM.

    uv run python governance/rls_check.py [--env dev]
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

ap = argparse.ArgumentParser()
ap.add_argument("--env", default=os.environ.get("DEMO_ENV", "dev"))
env = ap.parse_args().env
admin = WorkspaceClient()
wh = next(iter(admin.warehouses.list())).id
sps = json.load(open(".localdata/test_sps.json"))

QUERIES = {
    "global layer, rows per country": f"SELECT country, count(*) AS rows FROM pos_global.{env}_gold.sales_by_country_hour GROUP BY country ORDER BY country",
    "PH store-level gold": f"SELECT count(*) AS rows, count(DISTINCT store_id) AS stores FROM pos_ph.{env}_gold.sales_by_store_hour",
    "PH customer gold (PII masked?)": f"SELECT loyalty_id FROM pos_ph.{env}_gold.customer_daily LIMIT 2",
    "SG store-level gold": f"SELECT count(*) FROM pos_sg.{env}_gold.sales_by_store_hour",
    "PH silver (raw detail)": f"SELECT count(*) FROM pos_ph.{env}_silver.pos_lines",
}
EXPECT = {
    "sp-demo-mgr-ph": {"global layer, rows per country": "only PH", "PH store-level gold": "allowed",
                       "PH customer gold (PII masked?)": "masked", "SG store-level gold": "denied",
                       "PH silver (raw detail)": "denied"},
    "sp-demo-global-exec": {"global layer, rows per country": "all 4 countries", "PH store-level gold": "denied",
                            "PH customer gold (PII masked?)": "denied", "SG store-level gold": "denied",
                            "PH silver (raw detail)": "denied"},
}


def run_as(w: WorkspaceClient, sql: str):
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=wh, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        return "ERROR", (r.status.error.message or "")[:110]
    return "OK", (r.result.data_array or []) if r.result else []


def verdict(expect: str, status: str, rows) -> bool:
    if expect == "denied":
        return status == "ERROR"
    if status != "OK":
        return False
    if expect == "only PH":
        return [r[0] for r in rows] == ["PH"]
    if expect == "all 4 countries":
        return sorted(r[0] for r in rows) == ["DE", "PH", "SG", "US"]
    if expect == "masked":
        return bool(rows) and all(str(r[0]).startswith("anon_") for r in rows)
    return True


ok_all = True
for name, sp in sps.items():
    secret = admin.api_client.do("POST", f"/api/2.0/accounts/servicePrincipals/{sp['id']}/credentials/secrets", body={})
    try:
        w = WorkspaceClient(host=admin.config.host, client_id=sp["application_id"], client_secret=secret["secret"],
                            auth_type="oauth-m2m")
        print(f"\n=== {name} (group {sp['group']}) as {w.current_user.me().user_name} ===")
        for label, sql in QUERIES.items():
            status, rows = run_as(w, sql)
            exp = EXPECT[name][label]
            good = verdict(exp, status, rows)
            ok_all &= good
            shown = rows if status == "OK" else rows
            print(f"  [{'PASS' if good else 'FAIL'}] {label:32} expect {exp:16} -> {status}: {shown}")
    finally:
        admin.api_client.do("DELETE", f"/api/2.0/accounts/servicePrincipals/{sp['id']}/credentials/secrets/{secret['id']}")
print("\nPASS" if ok_all else "\nFAIL")
sys.exit(0 if ok_all else 1)
