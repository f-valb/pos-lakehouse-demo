# Power BI connection (not built in the demo)

Power BI reports can't be built from code in this repo. The demo stands in for them with the AI/BI dashboard `dashboards/pos_bi.lvdash.json`, which reads the same tables Power BI would.

## What Power BI connects to

| Use | Object | Notes |
|---|---|---|
| Global exec report | `pos_global.prod_gold.sales_by_country_hour`, `data_as_of`, `bi_forecast_vs_actual`, `bi_churn_risk` | Aggregated, no PII. Row filter `rf_global`: execs see all countries, managers see their own |
| Country manager report | `pos_<cc>.prod_gold.sales_by_store_hour`, `churn_predictions`, `demand_forecast`, `data_as_of` | Row filter `rf_country`; `loyalty_id` masked unless the user is in `pii_reader_<cc>` |
| Freshness banner on every page | `data_as_of` (`data_as_of`, `minutes_behind`, `status`) | Show "Data as of 10:35 UTC (4 min)" on every page. The SLA is 15 min |

## Setup

1. **SQL warehouse.** Use a serverless Pro warehouse per country workspace, plus one for the global workspace (`infra/terraform/databricks`: `databricks_sql_endpoint.bi`). Settings:
   - Min clusters 1, max 4 for about 5,000 users.
   - Auto-stop 10 min.
   - Get the Server hostname and HTTP path from *Connection details*.
2. **Connector.** In Power BI Desktop, use *Get data → Azure Databricks / Databricks*, enter the hostname and HTTP path, and choose *Azure Active Directory / Microsoft Entra ID* authentication.
3. **Entra SSO pass-through.** In the Power BI service, set the dataset's data source credentials to **OAuth2 + "Report viewers can only access this data source with their own Power BI identities using DirectQuery"** (SSO).
   - Each viewer's Entra identity reaches Databricks, so UC row filters and column masks apply per user.
   - No shared service account is needed, and nothing is re-implemented as Power BI RLS.
   - Entra groups are SCIM-synced to the Databricks account groups `mgr_<cc>`, `global_exec` and `pii_reader_<cc>`.
4. **Gateway.** None for Databricks on AWS with public endpoints. With PrivateLink, use a VNet data gateway or an on-premises gateway inside the network.

## Import vs DirectQuery

| | DirectQuery (recommended here) | Import |
|---|---|---|
| Freshness | Every visual queries the warehouse, so data is as fresh as gold (within 15 min) | Only as fresh as the last scheduled refresh: 8 a day on Pro, 48 on Premium, more via the XMLA/API |
| Security | **UC row filters and masks apply per viewer (SSO)** | Data is cached under the refresh identity, so RLS has to be rebuilt in Power BI |
| 5,000 users | Warehouse load scales with users. Mitigate with pre-aggregated gold, result caching, max clusters 4+, and Power BI *aggregations* (an Import aggregate table on top of DirectQuery detail) | Best interactive speed; no warehouse load at view time |
| Cost | Warehouse uptime during business hours | Refresh compute plus Power BI capacity |

**Recommendation:** DirectQuery with SSO for anything that must honour residency and row-level security, which is all detail data. Use **dual / aggregations** for the global exec page: an Import table of `sales_by_country_hour` refreshed every 15 min holds no PII and covers the heavy page. Set the report's *automatic page refresh* to 5 minutes; this is the "5 min BI refresh" part of the latency budget.

## Latency budget (target 15 min from sale to visual)

| Stage | Budget | Demo measurement |
|---|---|---|
| Poll: sale to landed file (window close, about 45 s lag, pull) | about 5 min | `pipeline_latency.poll_p50/p95` |
| Process: bronze, silver, gold (triggered every 5 min) | about 5 min | `bronze_p50 + silver_p50 + gold_p50` |
| BI: automatic page refresh or DirectQuery | about 5 min | Not measured in the demo (no Power BI) |
