# 10-minute live demo

**Before you start (T-15 min):** have the following already running.

```bash
docker compose up -d                                                    # LocalStack
uv run uvicorn mock_pos_api.app:app --port 8080 --log-level warning     # tab 1: legacy API
POLLER_SINKS=s3,volume uv run python -m ingestion.run_local run          # tab 2: planner + poller
watch -n 15 uv run python -m ingestion.run_local status                  # tab 3: ingestion status
```

The `medallion_refresh` job runs every 5 minutes in dev. Open these in the browser:
- the BI dashboard (`databricks bundle summary` → `pos_bi`),
- the FinOps dashboard,
- Catalog Explorer on `pos_ph`,
- the `ml_train` job.

---

## 0:00 – 1:00  The problem (no screen)

- The POS data comes from a fragile, pull-only REST API. It has no CDC, no "updated since" filter, and it crashes under load. Data is 24 hours late today; the target is 15 minutes.
- About 100M lines per hour across countries, with legacy formats and duplicates. Each country's data must stay in-country.
- "I'll show it scaled down to about 50k lines per minute and four countries. The design is the same at 28k per second."

## 1:00 – 2:30  The API is fragile, and the poller respects it

```bash
uv run python -m mock_pos_api.hammer --concurrency 30 --rounds 2
```

- A naive client gets an OOM, then 500 for everything until the service restarts.
- In tab 3, the poller pulls the same API with `crashes=0`. Point out: max 4 in flight (the API's soft limit is 6), adaptive windows (US at 60 s, SG at 7 min, sized to the record rate), re-pulls 21 minutes later for late voids, and an empty DLQ.
- **Key line:** "The output key is a pure function of the task, and writes are conditional, so a retry can never duplicate or change a file." Optionally run `uv run python scripts/idempotency_check.py`, which takes about a minute: it kill -9s the poller mid-run and shows identical SHA-256 hashes.

## 2:30 – 4:30  Lakehouse: bronze, silver, gold in 15 minutes

In Databricks, open the `pos-ph-medallion-dev` pipeline graph:
- **Bronze:** Auto Loader, append-only, every pulled version kept.
- **Silver:** translation (four date formats and timezones, the DE decimal comma, tender and status codes), 8 named data-quality rules, quarantine, and AUTO CDC on (store, register, txn, line) sequenced by the pull timestamp.

```sql
SELECT failed_rules, count(*), first(_raw_line) FROM pos_ph.dev_silver.pos_lines_quarantine GROUP BY 1;
SELECT * FROM pos_global.dev_gold.pipeline_latency;   -- e2e p95 about 12 min, per stage
SELECT * FROM pos_global.dev_gold.data_as_of;         -- the freshness every BI page shows
```

Then show `uv run python scripts/dedupe_check.py --country PH`. Two re-pulls of the same window add about 80k rows to bronze and zero to silver, and late voids are applied.

## 4:30 – 6:00  Governance: residency, RLS, masks

```bash
uv run python governance/rls_check.py
```

- **PH manager:** sees only PH in the global layer, PH store-level gold, and `loyalty_id` as `anon_…`. Gets *denied* on SG and on silver.
- **Global exec:** sees all four countries, aggregated only. Gets *denied* on every country catalog.
- These are real service principals, standing in for Entra ID users synced by SCIM.
- Talking point: "In prod each country is its own metastore. Only aggregates cross the border, via Delta Sharing. Detail sharing and Federation are the documented alternatives, with their trade-offs" (`governance/README.md`).

## 6:00 – 7:30  ML: churn and forecast, deploy code not models

- **`ml_train` run:** per-country `for_each` training. Champion vs challenger on a customer-hash holdout. Point at the US: "the gate kept v2 because v4 was worse."
- **`pos_ph.dev_ml.churn_model` in Catalog Explorer:** versions, the `@champion` and `@challenger` aliases, and the `previous_champion` tag.
- **Rollback:**

```bash
databricks bundle run ml_rollback -t dev --params country=ph   # champion moves back to its previous version, no retrain
```

- **Forecast:** one pyfunc per country wrapping 480–720 per-store × category models (many-models pattern via `applyInPandas`).

## 7:30 – 9:30  Failure injection (scripted)

```bash
uv run python scripts/failure_demo.py --step peak    --minutes 2
uv run python scripts/failure_demo.py --step outage  --minutes 3
uv run python scripts/failure_demo.py --step restore --minutes 3
```

1. **Peak.** API limits drop and store traffic takes capacity. The 503s appear; AIMD halves the poller's concurrency and the windows shrink. `crashes=0`.
2. **SG outage.** The breaker opens after 3 failures, and live SG windows become *gaps* instead of retries. `pos-SG-circuit-open` and `pos-SG-ingest-freshness` go to ALARM. In the BI dashboard, SG's `minutes behind` grows and its status turns **STALE**. The other countries stay fresh.
3. **Restore.** A half-open probe succeeds and live SG windows resume *first*, so freshness recovers immediately. The gap ledger drains **newest-first**, at most 3 windows per minute on the low-priority queue. Live windows always win the concurrency slots.

## 9:30 – 10:00  Ops and FinOps, wrap-up

- **DevOps:** the PR workflow runs lint, unit tests including local Spark, and bundle validate, then deploys to dev. Main deploys to staging, runs the integration tests, then opens an **approval issue** and waits for an `approve` comment before deploying to prod.
- **FinOps dashboard:** spend by env, component and country tag.
  - Budgets *alert*; policies and quotas *enforce* (serverless budget policy, cluster policy, warehouse max clusters, `max_concurrent_runs`).
  - Free Edition emits no billing, so the billed panel is empty, and the runtime estimate next to it is labelled as an estimate.
- **Close with gaps vs production:** the README's "real vs simulated" table.
