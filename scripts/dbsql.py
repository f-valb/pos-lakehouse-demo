"""Run a templated SQL file against a Databricks SQL warehouse (Statement Execution API).

    uv run python scripts/dbsql.py governance/00_bootstrap.sql --countries ph,sg,de,us --envs dev,staging,prod
    uv run python scripts/dbsql.py -e "SELECT current_user()"

Templating: lines between `-- @for_each_country` / `-- @end` are repeated per country with ${cc}/${CC};
lines between `-- @for_each_env` / `-- @end` are repeated per env with ${env}. ${env} outside a block
uses --env. Auth comes from ~/.databrickscfg (DATABRICKS_CONFIG_PROFILE), never from this file.
"""

from __future__ import annotations

import argparse
import os
import re
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


def warehouse_id(w: WorkspaceClient) -> str:
    if os.environ.get("DATABRICKS_WAREHOUSE_ID"):
        return os.environ["DATABRICKS_WAREHOUSE_ID"]
    whs = list(w.warehouses.list())
    if not whs:
        sys.exit("no SQL warehouse found")
    return whs[0].id


def expand(sql: str, countries: list[str], envs: list[str], env: str) -> str:
    def block(tag, items, subst):
        pat = re.compile(rf"^-- @{tag}\n(.*?)^-- @end\n", re.S | re.M)
        return lambda text: pat.sub(lambda m: "".join(subst(m.group(1), it) for it in items), text)

    # Country blocks may be nested inside env blocks (never the reverse): expand inner first.
    sql = block("for_each_country", countries, lambda t, c: t.replace("${cc}", c).replace("${CC}", c.upper()))(sql)
    sql = block("for_each_env", envs, lambda t, e: t.replace("${env}", e))(sql)
    return sql.replace("${env}", env)


def split(sql: str) -> list[str]:
    stmts, cur, depth = [], [], 0
    for line in sql.splitlines():
        if line.strip().startswith("--") and not cur:
            continue
        cur.append(line)
        depth += line.count("(") - line.count(")")
        if line.rstrip().endswith(";") and depth <= 0:
            s = "\n".join(cur).strip().rstrip(";").strip()
            if s:
                stmts.append(s)
            cur, depth = [], 0
    tail = "\n".join(cur).strip().rstrip(";")
    if tail and not all(ln.strip().startswith("--") or not ln.strip() for ln in tail.splitlines()):
        stmts.append(tail)
    return stmts


def run(w: WorkspaceClient, wh: str, stmt: str, show: bool = True):
    r = w.statement_execution.execute_statement(statement=stmt, warehouse_id=wh, wait_timeout="50s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(f"{r.status.state}: {r.status.error.message if r.status.error else ''}\n--> {stmt[:300]}")
    rows = r.result.data_array if r.result and r.result.data_array else []
    if show and rows:
        cols = [c.name for c in r.manifest.schema.columns]
        print(" | ".join(cols))
        for row in rows[:200]:
            print(" | ".join("" if v is None else str(v) for v in row))
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("file", nargs="?")
    ap.add_argument("-e", "--execute")
    ap.add_argument("--countries", default="ph,sg,de,us")
    ap.add_argument("--envs", default="dev,staging,prod")
    ap.add_argument("--env", default=os.environ.get("DEMO_ENV", "dev"))
    ap.add_argument("--continue-on-error", action="store_true")
    a = ap.parse_args()
    w = WorkspaceClient()
    wh = warehouse_id(w)
    sql = a.execute if a.execute else open(a.file).read()
    stmts = split(expand(sql, a.countries.split(","), a.envs.split(","), a.env))
    failed = 0
    for i, s in enumerate(stmts, 1):
        first = s.splitlines()[0][:110]
        try:
            run(w, wh, s)
            print(f"[{i}/{len(stmts)}] ok   {first}", file=sys.stderr)
        except RuntimeError as e:
            failed += 1
            print(f"[{i}/{len(stmts)}] FAIL {first}\n      {str(e).splitlines()[0]}", file=sys.stderr)
            if not a.continue_on_error:
                sys.exit(1)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
