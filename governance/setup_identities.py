"""Idempotent identity setup: account groups, workspace assignment, entitlements, memberships,
and the two test service principals used by rls_check.py.

PROD: none of this exists. Groups come from Entra ID through SCIM provisioning at the account
level (see governance/README.md), users sign in with Entra SSO, and group membership is
managed in Entra. This script plays the role of that sync for the demo.

    uv run python governance/setup_identities.py
"""

from __future__ import annotations

import json
import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

GROUPS = ["data_platform", "global_exec"] + [f"{p}_{c}" for p in ("mgr", "pii_reader") for c in ("ph", "sg", "de", "us")]
SQL_USERS = ["global_exec", "mgr_ph", "mgr_sg", "mgr_de", "mgr_us"]  # BI consumers need DBSQL access
TEST_SPS = {"sp-demo-mgr-ph": "mgr_ph", "sp-demo-global-exec": "global_exec"}
PATCH = "urn:ietf:params:scim:api:messages:2.0:PatchOp"

w = WorkspaceClient()
api = w.api_client.do


def find(kind: str, name: str):
    res = api("GET", f"/api/2.0/account/scim/v2/{kind}", query={"filter": f'displayName eq "{name}"'})
    return (res.get("Resources") or [None])[0]


def add_member(group_id: str, member_id: str):
    api("PATCH", f"/api/2.0/account/scim/v2/Groups/{group_id}",
        body={"schemas": [PATCH], "Operations": [{"op": "add", "path": "members", "value": [{"value": member_id}]}]})


def retry(fn, tries=6):
    # Workspace SCIM sees a freshly assigned account group only after a short delay.
    for i in range(tries):
        try:
            return fn()
        except NotFound:
            if i == tries - 1:
                raise
            time.sleep(2 * (i + 1))


ids = {}
for g in GROUPS:
    grp = find("Groups", g) or api("POST", "/api/2.0/account/scim/v2/Groups",
                                   body={"schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"], "displayName": g})
    ids[g] = grp["id"]
    api("PUT", f"/api/2.0/preview/permissionassignments/principals/{grp['id']}", body={"permissions": ["USER"]})
    if g in SQL_USERS:
        retry(lambda gid=grp["id"]: api("PATCH", f"/api/2.0/preview/scim/v2/Groups/{gid}", body={"schemas": [PATCH], "Operations": [
            {"op": "add", "path": "entitlements", "value": [{"value": "databricks-sql-access"}]}]}))
    print(f"group {g:16} {grp['id']}")

me = w.current_user.me()
add_member(ids["data_platform"], me.id)
print(f"{me.user_name} -> data_platform")

wh = next(iter(w.warehouses.list())).id
sps = {}
for name, group in TEST_SPS.items():
    sp = find("ServicePrincipals", name) or api("POST", "/api/2.0/account/scim/v2/ServicePrincipals", body={
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"], "displayName": name, "active": True})
    add_member(ids[group], sp["id"])
    api("PUT", f"/api/2.0/preview/permissionassignments/principals/{sp['id']}", body={"permissions": ["USER"]})
    w.permissions.update("warehouses", wh, access_control_list=[AccessControlRequest(
        service_principal_name=sp["applicationId"], permission_level=PermissionLevel.CAN_USE)])
    sps[name] = {"id": sp["id"], "application_id": sp["applicationId"], "group": group}
    print(f"service principal {name} -> {group}, CAN_USE on warehouse {wh}")

os.makedirs(".localdata", exist_ok=True)
json.dump(ids, open(".localdata/group_ids.json", "w"), indent=1)
json.dump(sps, open(".localdata/test_sps.json", "w"), indent=1)
print("note: new group memberships can take a few minutes to reach SQL warehouses / pipelines")
