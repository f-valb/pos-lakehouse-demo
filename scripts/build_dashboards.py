"""Generate the AI/BI dashboard definitions (dashboards/*.lvdash.json) deployed by the bundle.

    uv run python scripts/build_dashboards.py

Kept as code so the JSON stays consistent (field names == encodings, 12-col rows, versions).
BI dashboard queries use bare table names: the bundle sets dataset_catalog=pos_global and
dataset_schema=<env>_gold per target, so the same file works in dev / staging / prod.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "dashboards"
THEME = {
    "canvasBackgroundColor": {"light": "#F7F9FA", "dark": "#1F272D"},
    "widgetBackgroundColor": {"light": "#FFFFFF", "dark": "#11171C"},
    "widgetBorderColor": {"light": "#FFFFFF", "dark": "#11171C"},
    "fontColor": {"light": "#11171C", "dark": "#E8ECF0"},
    "selectionColor": {"light": "#2272B4", "dark": "#8ACAFF"},
    "visualizationColors": ["#0072B2", "#E69F00", "#009E73", "#CC79A7", "#56B4E9", "#D55E00", "#999999"],
    "widgetHeaderAlignment": "LEFT",
}
USD = {"type": "number-currency", "currencyCode": "USD", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}
COUNT = {"type": "number", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}


def ds(name, display, sql):
    return {"name": name, "displayName": display, "queryLines": [ln + "\n" for ln in sql.strip().splitlines()]}


def text(name, lines, x, y, w, h):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": lines}}, "position": {"x": x, "y": y, "width": w, "height": h}}


def fields(*pairs):
    return [{"name": n, "expression": e} for n, e in pairs]


def counter(name, title, dataset, value_name, value_expr, fmt=None, description=None, x=0, y=0, w=3, h=3):
    enc = {"value": {"fieldName": value_name, "displayName": title, **({"format": fmt} if fmt else {})}}
    frame = {"showTitle": True, "title": title}
    if description:
        frame |= {"showDescription": True, "description": description}
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": fields((value_name, value_expr)), "disaggregated": False}}],
        "spec": {"version": 2, "widgetType": "counter", "encodings": enc, "frame": frame}},
        "position": {"x": x, "y": y, "width": w, "height": h}}


def chart(name, kind, title, dataset, x_field, x_expr, x_type, y_name, y_expr, y_title, color=None,
          fmt=None, description=None, x=0, y=0, w=6, h=6, layout=None):
    q = [(x_field, x_expr), (y_name, y_expr)] + ([color] if color else [])
    yenc = {"fieldName": y_name, "displayName": y_title, "scale": {"type": "quantitative"}}
    if fmt:
        yenc["format"] = fmt
    enc = {"x": {"fieldName": x_field, "scale": {"type": x_type}, "displayName": x_field}, "y": yenc}
    if color:
        enc["color"] = {"fieldName": color[0], "scale": {"type": "categorical"}, "displayName": color[0]}
    frame = {"showTitle": True, "title": title}
    if description:
        frame |= {"showDescription": True, "description": description}
    spec = {"version": 3, "widgetType": kind, "encodings": enc, "frame": frame}
    if layout:
        spec["mark"] = {"layout": layout}
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": fields(*q), "disaggregated": False}}], "spec": spec},
        "position": {"x": x, "y": y, "width": w, "height": h}}


def table(name, title, dataset, cols, description=None, x=0, y=0, w=12, h=6):
    frame = {"showTitle": True, "title": title}
    if description:
        frame |= {"showDescription": True, "description": description}
    return {"widget": {"name": name, "queries": [{"name": "main_query", "query": {
        "datasetName": dataset, "fields": fields(*[(c, f"`{c}`") for c, _ in cols]), "disaggregated": True}}],
        "spec": {"version": 2, "widgetType": "table",
                 "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in cols]}, "frame": frame}},
        "position": {"x": x, "y": y, "width": w, "height": h}}


def multi_filter(name, title, field, datasets, x=0, y=0, w=12, h=2):
    queries = [{"name": f"{d}_{field}", "query": {"datasetName": d, "fields": fields((field, f"`{field}`")),
                                                  "disaggregated": False}} for d in datasets]
    return {"widget": {"name": name, "queries": queries, "spec": {
        "version": 2, "widgetType": "filter-multi-select",
        "encodings": {"fields": [{"fieldName": field, "displayName": title, "queryName": q["name"]} for q in queries]},
        "frame": {"showTitle": True, "title": title}}}, "position": {"x": x, "y": y, "width": w, "height": h}}


def bi_dashboard() -> dict:
    datasets = [
        ds("ds_freshness", "Freshness", """
SELECT country, data_as_of, latest_sale_utc,
  round((unix_timestamp(current_timestamp()) - unix_timestamp(data_as_of)) / 60, 1) AS minutes_behind,
  CASE WHEN (unix_timestamp(current_timestamp()) - unix_timestamp(data_as_of)) / 60 <= 15 THEN 'FRESH' ELSE 'STALE' END AS status
FROM data_as_of"""),
        ds("ds_sales_hourly", "Sales by country and hour", """
SELECT country, txn_hour_utc, sum(net_sales_usd) AS net_sales_usd, sum(transactions) AS transactions, sum(units) AS units
FROM sales_by_country_hour
WHERE txn_hour_utc >= timestampadd(HOUR, -48, current_timestamp())
GROUP BY country, txn_hour_utc"""),
        ds("ds_forecast", "Forecast vs actual", """
SELECT country, business_date, series, units FROM bi_forecast_vs_actual
WHERE business_date >= date_sub(current_date(), 30)"""),
        ds("ds_churn", "Churn risk", "SELECT country, as_of_date, risk_band, model_version, customers, avg_score FROM bi_churn_risk"),
        ds("ds_latency", "Latency per stage", """
SELECT country, records, poll_p50, poll_p95, bronze_p50, silver_p50, gold_p50, end_to_end_p50, end_to_end_p95, measured_at
FROM pipeline_latency ORDER BY country"""),
    ]
    layout = [
        text("title", ["## Retail POS: sales, forecast and churn (BI stand-in for Power BI)\n"], 0, 0, 12, 1),
        text("subtitle", ["Global aggregated layer (no PII). Every number shows how fresh it is: see *data as of* per "
                          "country. Store-level detail lives in the country catalogs, behind row filters.\n"], 0, 1, 12, 1),
        multi_filter("filter_country", "Country", "country", ["ds_freshness", "ds_sales_hourly", "ds_forecast", "ds_churn"], 0, 2, 12, 2),
        counter("kpi_sales", "Net sales, last 48h (USD)", "ds_sales_hourly", "sum(net_sales_usd)", "SUM(`net_sales_usd`)", USD, x=0, y=4),
        counter("kpi_txns", "Transactions, last 48h", "ds_sales_hourly", "sum(transactions)", "SUM(`transactions`)", COUNT, x=3, y=4),
        counter("kpi_behind", "Max minutes behind (SLA 15)", "ds_freshness", "max(minutes_behind)", "MAX(`minutes_behind`)",
                {"type": "number-plain", "decimalPlaces": {"type": "max", "places": 1}},
                description="Oldest data_as_of across selected countries", x=6, y=4),
        counter("kpi_high_risk", "High churn-risk members", "ds_churn", "sum(customers)",
                "SUM(CASE WHEN `risk_band` = 'HIGH' THEN `customers` ELSE 0 END)", COUNT, x=9, y=4),
        table("tbl_freshness", "Data as of (per country)", "ds_freshness",
              [("country", "Country"), ("data_as_of", "Data complete up to (UTC)"), ("latest_sale_utc", "Latest sale (UTC)"),
               ("minutes_behind", "Minutes behind"), ("status", "Status")], x=0, y=7, w=12, h=4),
        chart("ln_sales", "line", "Net sales per hour (USD)", "ds_sales_hourly", "txn_hour_utc", "`txn_hour_utc`", "temporal",
              "sum(net_sales_usd)", "SUM(`net_sales_usd`)", "Net sales (USD)", color=("country", "`country`"), fmt=USD,
              x=0, y=11, w=6, h=6),
        chart("ln_forecast", "line", "Daily units: actual vs 14-day forecast", "ds_forecast", "business_date", "`business_date`",
              "temporal", "sum(units)", "SUM(`units`)", "Units", color=("series", "`series`"), fmt=COUNT,
              description="Before the live stream started, actuals are SIMULATED history (demo).", x=6, y=11, w=6, h=6),
        chart("bar_churn", "bar", "Loyalty members by churn risk", "ds_churn", "country", "`country`", "categorical",
              "sum(customers)", "SUM(`customers`)", "Members", color=("risk_band", "`risk_band`"), fmt=COUNT,
              description="Batch-scored daily with the @champion model per country", x=0, y=17, w=6, h=6),
        table("tbl_latency", "End-to-end latency per stage (minutes, live records, last 60 min)", "ds_latency",
              [("country", "Country"), ("records", "Records"), ("poll_p50", "Poll p50"), ("poll_p95", "Poll p95"),
               ("bronze_p50", "Bronze p50"), ("silver_p50", "Silver p50"), ("gold_p50", "Gold p50"),
               ("end_to_end_p50", "E2E p50"), ("end_to_end_p95", "E2E p95")], x=6, y=17, w=6, h=6),
    ]
    return {"datasets": datasets, "pages": [{"name": "overview", "displayName": "Overview", "pageType": "PAGE_TYPE_CANVAS",
                                             "layoutVersion": "GRID_V1", "layout": layout}], "uiSettings": {"theme": THEME}}


def finops_dashboard() -> dict:
    datasets = [
        ds("ds_runtime", "Compute runtime (estimate)", """
SELECT usage_date, env, component, data_country, kind, workload, compute_minutes, est_cost_usd
FROM pos_global.finops.runtime_daily WHERE usage_date >= date_sub(current_date(), 30)"""),
        ds("ds_billing", "Billed usage (system.billing.usage)", """
SELECT usage_date, env, component, data_country, product, workload, dbus, list_cost_usd
FROM pos_global.finops.billing_daily WHERE usage_date >= date_sub(current_date(), 30)"""),
        ds("ds_tags", "Tag coverage", "SELECT usage_date, untagged_pct FROM pos_global.finops.tag_coverage"),
    ]
    layout = [
        text("title", ["## FinOps: spend by environment, tag and pipeline\n"], 0, 0, 12, 1),
        text("subtitle", ["Budgets **alert**; policies and quotas **enforce** (serverless budget policy tags, cluster "
                          "policy caps, warehouse max clusters, job max_concurrent_runs). Billed cost comes from "
                          "`system.billing.usage`; this Free Edition workspace emits no billable usage, so the runtime-based "
                          "**estimate** (compute minutes x USD 0.10) is shown next to it.\n"], 0, 1, 12, 2),
        multi_filter("filter_env", "Environment", "env", ["ds_runtime", "ds_billing"], 0, 3, 12, 2),
        counter("kpi_est", "Estimated cost, 30 days (USD)", "ds_runtime", "sum(est_cost_usd)", "SUM(`est_cost_usd`)", USD, x=0, y=5, w=4),
        counter("kpi_billed", "Billed list cost, 30 days (USD)", "ds_billing", "sum(list_cost_usd)", "SUM(`list_cost_usd`)", USD,
                description="system.billing.usage (empty on Free Edition)", x=4, y=5, w=4),
        counter("kpi_minutes", "Serverless compute minutes", "ds_runtime", "sum(compute_minutes)", "SUM(`compute_minutes`)", COUNT,
                x=8, y=5, w=4),
        chart("bar_env", "bar", "Estimated cost by environment and component", "ds_runtime", "env", "`env`", "categorical",
              "sum(est_cost_usd)", "SUM(`est_cost_usd`)", "USD (estimate)", color=("component", "`component`"), fmt=USD,
              x=0, y=8, w=6, h=6),
        chart("bar_country", "bar", "Estimated cost by data_country tag", "ds_runtime", "data_country", "`data_country`",
              "categorical", "sum(est_cost_usd)", "SUM(`est_cost_usd`)", "USD (estimate)", color=("kind", "`kind`"), fmt=USD,
              x=6, y=8, w=6, h=6),
        chart("ln_daily", "bar", "Daily estimated cost by component", "ds_runtime", "usage_date", "`usage_date`", "temporal",
              "sum(est_cost_usd)", "SUM(`est_cost_usd`)", "USD (estimate)", color=("component", "`component`"), fmt=USD,
              x=0, y=14, w=6, h=6),
        chart("bar_billed", "bar", "Billed list cost by environment and product", "ds_billing", "env", "`env`", "categorical",
              "sum(list_cost_usd)", "SUM(`list_cost_usd`)", "USD (list)", color=("product", "`product`"), fmt=USD,
              description="Populates in workspaces with billing (Premium / Enterprise)", x=6, y=14, w=6, h=6),
        table("tbl_workload", "Spend by pipeline / job (estimate)", "ds_runtime",
              [("usage_date", "Date"), ("env", "Env"), ("component", "Component"), ("data_country", "Country"),
               ("kind", "Kind"), ("workload", "Pipeline / job"), ("compute_minutes", "Minutes"), ("est_cost_usd", "Est. USD")],
              x=0, y=20, w=12, h=7),
    ]
    return {"datasets": datasets, "pages": [{"name": "finops", "displayName": "FinOps", "pageType": "PAGE_TYPE_CANVAS",
                                             "layoutVersion": "GRID_V1", "layout": layout}], "uiSettings": {"theme": THEME}}


if __name__ == "__main__":
    OUT.mkdir(exist_ok=True)
    for name, d in [("pos_bi", bi_dashboard()), ("finops", finops_dashboard())]:
        (OUT / f"{name}.lvdash.json").write_text(json.dumps(d, indent=1) + "\n")
        print(f"wrote dashboards/{name}.lvdash.json ({len(d['datasets'])} datasets, {len(d['pages'][0]['layout'])} widgets)")
