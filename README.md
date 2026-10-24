# POS → Lakehouse demo

This is a scaled-down but architecturally faithful build of a global retailer's POS-to-lakehouse platform. It moves data from a fragile, pull-only legacy POS API into a medallion lakehouse on Databricks, with per-country data residency, BI and ML. End-to-end latency is 15 minutes, down from 24 hours.

- [`docs/architecture.md`](docs/architecture.md): diagrams, design decisions and trade-offs, and scaling to 100M records per hour.
- [`docs/demo_script.md`](docs/demo_script.md): the 10-minute live walkthrough, including failure injection.
- [`governance/README.md`](governance/README.md): the access model, Entra ID SSO/SCIM, residency, and the Delta Sharing / Federation alternatives.
- [`powerbi/README.md`](powerbi/README.md): Power BI connection, SSO, and Import vs DirectQuery.

```
mock_pos_api/   legacy API mock (FastAPI): legacy formats, duplicates, late voids, crashes under load
ingestion/      planner + poller: AIMD, circuit breaker, backoff, DLQ, adaptive windows, idempotent S3 writes
pipelines/      Lakeflow declarative pipelines: one per country (bronze/silver/gold) + global aggregates
governance/     UC layout, row filters, column masks, grants, identities, RLS acceptance test
ml/             churn + many-models forecast: features, MLflow, UC registry, gate, batch scoring, drift, rollback
finops/         cost attribution views; dashboards/ has the BI stand-in and the FinOps dashboard
infra/          Terraform (AWS per country, Databricks per country) + Lambda image. Validated, not applied
resources/      Databricks Asset Bundle resources (pipelines, jobs, alerts, dashboards); databricks.yml has the targets
.github/        CI (PR) and release (staging → manual approval → prod)
tests/          unit (incl. local Spark) and integration (against a deployed environment)
scripts/        dbsql runner, acceptance checks, failure demo, dashboard builder, teardown
```

## What is real and what is simulated

| Area | In this demo | In production | Gap / what changes |
|---|---|---|---|
| POS API | `mock_pos_api` (FastAPI) on localhost. Deterministic synthetic data in legacy formats, with duplicates, late voids, malformed rows, concurrency limits, crashes, peak mode and per-region outages | The retailer's legacy API over PrivateLink or VPN | Limits, latency and formats have to be measured on the real API, and window sizes tuned to them |
| Volume | About 50k lines/min across 4 countries (`POS_API_RECORDS_PER_MINUTE`) | About 100M lines/h (28k/s) | Same code; see the scaling table in `docs/architecture.md` |
| AWS | **LocalStack** in Docker: S3, SQS + DLQ, CloudWatch metrics and alarms, SNS. Planner and poller run as local processes (`ingestion/run_local.py`) | EventBridge Scheduler → planner Lambda → SQS → poller Lambda (reserved concurrency) → S3, DynamoDB control store | Terraform in `infra/terraform/aws` is **validated but not applied** (no AWS account). The Lambda handlers and DynamoDB store exist, but only the local path ran |
| Landing to Databricks | The poller writes each file to LocalStack S3 **and** to a UC Volume through the Files API, because Free Edition can't reach a laptop | Auto Loader reads the country S3 bucket through an external location / external volume with **managed file events** | The Volume sink and directory-listing Auto Loader are demo-only |
| Countries / residency | 4 UC catalogs (`pos_ph`, `pos_sg`, `pos_de`, `pos_us`) + `pos_global` in **one** Free Edition workspace and metastore. One pipeline per country | One workspace and metastore per country, in that country's AWS account and region. The global layer is fed by Delta Sharing of aggregates | One workspace can't show the separate-metastore boundary. The global pipeline reads country gold directly instead of through a share |
| Environments | `dev_`, `staging_`, `prod_` schema prefixes in the same catalogs. Bundle targets dev, staging and prod against the same workspace | A separate workspace per environment (and per country), each target with its own host and service principal | Dev and staging were deployed and run. Prod is a target the release workflow deploys, gated by approval |
| Identity | **Account groups** created through the SCIM API by `setup_identities.py`. Two test **service principals** stand in for users | Entra ID SSO + SCIM provisioning, Entra groups mapped 1:1 | No Entra tenant. The mapping is documented in `governance/README.md` |
| BI | **AI/BI dashboard** (`dashboards/pos_bi.lvdash.json`) over the same gold tables | Power BI (DirectQuery + Entra SSO) for about 5,000 users | Power BI can't be built from code; see `powerbi/README.md` |
| ML history | **Simulated 180-day history** (`ml/ml_lib/history.py`), consistent with the live stream through shared customer profiles (`common/customers.py`) | Real silver/gold history | The live loyalty share is small (about 0.05% of transactions) so that live behaviour matches the simulated member base |
| FinOps | `system.billing.usage` views and dashboard panels exist but are **empty**, because Free Edition emits no billable usage. A **runtime-based estimate** from `system.lakeflow` (compute minutes × USD 0.10, labelled as an estimate) is shown next to them | `system.billing.usage` × `list_prices`, serverless budget policies, budgets | Budget policies, budgets and cluster policies are Terraform only (no account console on Free Edition) |
| Monitoring | Custom PSI + data-quality job (alerting and retrain trigger) **and** Lakehouse Monitoring (inference-log and snapshot monitors) | Same | None |
| CI/CD | Workflows written. Staging deploy and integration tests were **run locally** with the same commands | GitHub Actions with OIDC federation to a Databricks SP; the `prod` environment requires reviewers | Running the workflows needs a GitHub repo plus environment and OIDC setup (see below) |

## Acceptance criteria

The checks were run in this workspace on 2026-10-24 and 2026-10-25 (UTC). The commands to reproduce each one are in the next table.

| # | Criterion | Result | Evidence |
|---|---|---|---|
| 1 | The mock API crashes when hammered directly; the poller does not crash it | **PASS** | `mock_pos_api.hammer` (30 concurrent): OOM crash, then 500 on every request. The poller ran for hours with `crashes=0` and max 4 in flight. At peak, with store traffic taking capacity, the API returned 503s at an 18–23% error rate; AIMD backed off, windows shrank (PH 180→120 s, SG 420→240 s) and the API never crashed (`crashes=0`) |
| 2 | Re-running the same window produces no duplicate rows in silver | **PASS** | `scripts/dedupe_check.py`: 2 extra pulls of a window → bronze +80,572 rows, silver unchanged at 20,093 rows = 20,093 distinct keys, and 302 late voids applied. Repeated on staging by `tests/integration` |
| 3 | Killing the poller mid-run and retrying produces the same S3 files | **PASS** | `scripts/idempotency_check.py`: SIGKILL with 6/24 files written, SQS redelivery, resume → all 24 keys, SHA-256 identical to a clean run, DLQ empty |
| 4 | End-to-end time under 15 min at demo volume, per stage | **PASS** | `pos_global.dev_gold.pipeline_latency`: e2e p95 11.9–12.5 min (p50 7.7–10.3). Stages: poll p50 2.6–4.3 min, bronze 1.1–2.2, silver about 0.1, gold 3.5–4.2 (gold includes the wait for the next 5-min refresh). The BI refresh stage isn't measured (no Power BI) |
| 5 | A `pos_ph` manager sees only PH rows; `global_exec` sees only the aggregated layer | **PASS** | `governance/rls_check.py`: 10/10 checks as two real service principals (output below) |
| 6 | Bad rows land in quarantine, not silver | **PASS** | All 5 defect types are quarantined with their rule names (`valid_qty`, `valid_txn_ts`, `valid_price`, `known_tender`, `has_store`). Integration test: silver + distinct quarantined = distinct pulled lines |
| 7 | A new model version flows challenger → validation → champion, and an alias rollback works | **PASS** | `ml_train`: v2 beat v1 0.8596 vs 0.8193 on the same holdout → champion. US v4 was rejected (0.8666 vs 0.8898) and US kept v2. `ml_rollback --params country=ph` moved `@champion` from v8 to v7 (the `previous_champion` tag), tagged v8 `rolled_back`, and the next scoring run used v7 (`scripts/model_versions.py`) |
| 8 | Injected drift triggers the monitor and an alert | **PASS** | With no injection, max PSI was 0.03 and the retrain task was skipped. With `inject_drift=ph`, only PH breached (avg basket PSI 2.34, spend 1.0–1.1, card share 0.96); the condition task triggered `ml_train` for the drifted country only (`countries_json=["sg"]` in the SG rerun), and the `ml_drift` DBSQL alert went to **TRIGGERED** (email). The freshness alert also fired for real (TRIGGERED, email) during the SG outage |
| 9 | CI runs on a PR and the prod deploy waits for manual approval | **NOT RUN** | The workflows are written and the bundle validates for every target. Running them needs a GitHub repo, a `prod` environment with required reviewers, and an OIDC federation policy. Not done because it publishes the repo |
| 10 | The FinOps dashboard shows spend by environment and tag | **PARTIAL** | It shows *estimated* spend by env, component, country and workload from `system.lakeflow`. Billed spend (`system.billing.usage`) is empty on Free Edition |
| 11 | The README lists every substitution and every gap versus production | **PASS** | This file, the table above |

### Reproduce

| What | Command |
|---|---|
| Unit tests (incl. local Spark) | `uv run pytest -q` |
| API crash vs poller | `uv run python -m mock_pos_api.hammer`, then `uv run python -m ingestion.run_local status` |
| Idempotency | `uv run python scripts/idempotency_check.py` |
| Silver dedupe | `uv run python scripts/dedupe_check.py --country PH` |
| Latency | `uv run python scripts/dbsql.py -e "SELECT * FROM pos_global.dev_gold.pipeline_latency"` |
| RLS / masks | `uv run python governance/rls_check.py` |
| ML gate | `databricks bundle run ml_train -t dev` then `uv run python scripts/job_outputs.py ml_train` |
| Rollback | `databricks bundle run ml_rollback -t dev --params country=ph` |
| Drift | `databricks bundle run ml_score_and_monitor -t dev --params inject_drift=ph` |
| Staging integration | `databricks bundle deploy -t staging && uv run pytest -m integration tests/integration --env staging` |
| Failure injection | `uv run python scripts/failure_demo.py`. Outage: SG breaker open → gaps, `pos-SG-circuit-open` ALARM. Restore: one probe, circuit closed within about 2 min, 5 gaps drained newest-first, alarm cleared |

## How to run

**Prerequisites:**
- macOS or Linux with Docker.
- [uv](https://docs.astral.sh/uv/) (Python 3.11).
- JDK 17 (only for the local Spark tests).
- Databricks CLI ≥ 1.0. Run `databricks auth login --host <workspace-url> --profile DEFAULT`; OAuth, no tokens in files.
- Terraform ≥ 1.6 (only for `terraform validate`).

```bash
uv sync                                                    # Python deps
databricks aitools install --agents claude-code --scope project --skills-only   # optional: Databricks agent skills
docker compose up -d                                       # LocalStack (S3, SQS, CloudWatch, SNS)
uv run python -m ingestion.run_local setup                 # buckets, queues + DLQ, alarms

# Databricks: UC layout, identities, security functions, grants
uv run python scripts/dbsql.py governance/00_bootstrap.sql
uv run python governance/setup_identities.py               # account groups, test SPs (membership takes minutes to propagate)
uv run python scripts/dbsql.py governance/10_security_functions.sql
uv run python scripts/dbsql.py governance/20_grants.sql
uv run python scripts/dbsql.py finops/finops_views.sql

# Run the stream
uv run uvicorn mock_pos_api.app:app --port 8080 --log-level warning               # terminal 1
POLLER_SINKS=s3,volume uv run python -m ingestion.run_local run                    # terminal 2

# Deploy and run the lakehouse
databricks bundle deploy -t dev                            # 5 pipelines, refresh job (every 5 min), ML jobs, alerts, dashboards
databricks bundle run medallion_refresh -t dev
uv run python scripts/dbsql.py governance/30_bi_views.sql --envs dev
databricks bundle run ml_seed_history -t dev && databricks bundle run ml_train -t dev
databricks bundle run ml_score_and_monitor -t dev
uv run python ml/setup_lakehouse_monitor.py --country ph --env dev
```

**CI/CD setup** (not done; it needs your GitHub repo):
1. Push to GitHub.
2. Create environments `dev`, `staging` and `prod`. Add **required reviewers** to `prod`.
3. Set the repo variables `DATABRICKS_HOST` and `DATABRICKS_CLIENT_ID` (a deployment service principal).
4. Add a federation policy on that SP that trusts `token.actions.githubusercontent.com` for this repo.

## Cost

| Item | Demo cost | Notes |
|---|---|---|
| Databricks Free Edition | **USD 0** | Serverless only, with quota limits. Jobs queue for about 1–2 min when quota is busy |
| LocalStack | USD 0 | Community image, local Docker |
| Same build on a paid serverless workspace | Roughly **USD 3–6 per hour while the 5-min refresh runs** | From `pos_global.finops.runtime_daily`: about 5 pipelines × 1–1.5 min of compute per 5-min cycle, at an assumed USD 0.10 per compute-minute. ML jobs add about USD 2–4 per daily run. **Pause the schedule when not demoing** (`databricks bundle deploy -t dev --var pause_status=PAUSED`) |
| AWS at demo volume (if the Terraform were applied) | Under USD 10/month | Lambda invocations each minute, SQS, S3 (7-day raw expiry), 14 alarms × USD 0.10, DynamoDB on demand |

**Teardown:** `./scripts/teardown.sh` asks before each step. It stops local processes and LocalStack, destroys the bundle in all targets, drops the `pos_*` catalogs, deletes the experiments, and deletes the demo groups and service principals.
