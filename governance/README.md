# Governance

| File | What it does |
|---|---|
| `00_bootstrap.sql` | Catalog per country (`pos_ph`, `pos_sg`, `pos_de`, `pos_us`) + `pos_global`; `<env>_bronze/_silver/_gold/_ml` schemas; landing volumes; catalog tags |
| `10_security_functions.sql` | Row filters (`rf_country`, `rf_global`) and column masks (`mask_pii`, `mask_email`) as UC SQL functions |
| `20_grants.sql` | Least-privilege grants per group; predictive optimization per catalog |
| `setup_identities.py` | Demo stand-in for Entra ID SCIM: account groups, workspace assignment, entitlements, two test service principals |
| `rls_check.py` | Acceptance test: runs the same queries as a PH manager and as a global exec |

Apply in order: `scripts/dbsql.py governance/00_bootstrap.sql`, `setup_identities.py`, `scripts/dbsql.py governance/10_security_functions.sql`, `scripts/dbsql.py governance/20_grants.sql`. The masks and row filters are bound in the pipeline table definitions (`schema=... MASK ...`, `row_filter=...`), so the functions must exist before the first pipeline run.

## Access model

| Group (Entra ID) | Country catalog `pos_<cc>` | `pos_global` | PII |
|---|---|---|---|
| `data_platform` (engineers, pipeline/job SPs) | all | all | raw |
| `mgr_<cc>` | gold of own country only | gold, row filter = own country | masked (`anon_<hash>`, `****@domain`) |
| `pii_reader_<cc>` (CRM, ML job SP) | silver + gold of own country | none | raw |
| `global_exec` | none | gold, all countries | none (aggregated only) |

Three things enforce this:
- **Grants** decide which layers a group can reach at all. A global exec has no `USE CATALOG` on any country catalog.
- **Row filters** are bound to the tables, so they apply however the table is queried (Power BI, SQL, notebooks, Genie). `rf_global` gives managers only their own country's rows in the global layer.
- **Column masks** on `loyalty_id` and `customer_email` in silver and gold. The pseudonym is a stable hash, so masked users can still count distinct customers.

## Entra ID SSO and SCIM (production)

1. **SSO.** Use the Entra ID enterprise app for Databricks with OIDC/SAML at the account level. Users sign in with corporate identities, and conditional access and MFA stay in Entra.
2. **SCIM.** Use Entra automatic provisioning to the Databricks account SCIM endpoint (`/api/2.0/accounts/{account_id}/scim/v2`). Users and groups are created as account principals, which is what Unity Catalog grants need. Workspace-local groups cannot be granted in UC; the demo hit exactly this and switched to account groups.
3. **Mapping.** Each Entra security group maps 1:1 to a Databricks account group of the same name. Examples: `sg-dbx-mgr-ph` maps to `mgr_ph`, and `sg-dbx-global-exec` maps to `global_exec`. Membership is managed only in Entra, and it reaches Databricks within the SCIM sync interval of about 40 minutes, or immediately with on-demand provisioning.
4. **Workspace assignment** is per group, from the account console or Terraform (`databricks_mws_permission_assignment`). So is the entitlement `databricks-sql-access` for BI users.
5. **Power BI** passes the signed-in user's Entra identity through to the SQL warehouse (SSO in the connector). Row filters and masks therefore apply per BI user. No shared service account is needed, and filters don't have to be duplicated in Power BI.

In the demo, the Entra side is simulated: `setup_identities.py` creates the groups and memberships through the account SCIM API.

## Data residency

**Default design: an aggregated global layer.**
- Every country's raw, silver, gold and ML data stays in that country's catalog. In production that catalog lives in its own metastore and workspace, in its own AWS region and account.
- Only the country gold *aggregates* are published to the global workspace, with no PII and no rows below country × hour × category. Each country publishes them with Delta Sharing (Databricks-to-Databricks). The global pipeline (`pipelines/pos_global`) reads the shared tables and builds `pos_global`.

**In the demo, all catalogs share one metastore.** So the global pipeline reads the country gold tables directly. This is the one place the demo cannot show the separate-metastore boundary.

### Alternatives (documented here, not built)

| | Global aggregated layer (default) | Delta Sharing of detail tables | Lakehouse Federation |
|---|---|---|---|
| What crosses the border | Aggregates only (no PII) | Detail rows, filtered by sharing partitions / row filters / views | Query results, at query time |
| Data copied out of country | Aggregates only | No copy, but detail is *readable* abroad (stored in-country) | No copy, but results leave the country per query |
| Latency to global users | Gold refresh (minutes) | Live (reads the provider's current version) | Live, but each query hits the source |
| Performance at 5,000 BI users | Best (local, pre-aggregated, cacheable) | Good; cross-region egress per read | Worst; every query crosses regions, source warehouse load |
| Legal fit | Strongest: no personal data leaves | Depends on whether "viewing from abroad" counts as transfer under that country's law | Weakest for strict residency; easiest for ad-hoc analysis |
| Ops | One pipeline per hop | Shares + recipients per country | Connections + credentials per source |
| When to choose | Default for exec / cross-region BI | When regional teams need row-level drill-down across borders and legal says remote access is OK | For occasional ad-hoc queries over sources you don't want to ingest, not for a 5,000-user BI tier |

**Delta Sharing notes.** Share only gold views, with partition filters by country. Use a recipient per consuming workspace, and audit access via `system.access.audit`. Sharing on the recipient's own region avoids egress for repeated BI reads, but it materializes a copy, which is a residency decision.

**Federation notes.** UC still governs federated catalogs, with grants and lineage. But compute runs where the query runs, and pushdown is limited. It is unsuitable as the serving path for the Power BI tier.

## Lineage, audit, discovery

- **Lineage.** UC lineage is captured automatically for pipelines, jobs, notebooks and DBSQL, down to column level. Example path: `pos_lines_raw` → `pos_lines` → `sales_by_store_hour` → `pos_global.sales_by_country_hour` → dashboard. Query it with `system.access.table_lineage` / `column_lineage`, or see it in Catalog Explorer.
- **Audit.** Grants, row-filter evaluation and share access are in `system.access.audit`.
- **Tags.** Catalogs carry `data_country`, `data_classification` and `cost_center`. These same tags drive FinOps reporting (see `finops/`).
