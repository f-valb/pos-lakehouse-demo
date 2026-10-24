"""Gold (per country): BI-facing aggregates, freshness and latency. Materialized views over
silver, refreshed by the same triggered pipeline update (incrementally where possible)."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CC = spark.conf.get("pos.country")
ENV = spark.conf.get("pos.env")
CAT = f"pos_{CC.lower()}"
SILVER = f"{CAT}.{ENV}_silver.pos_lines"
GOLD = f"{CAT}.{ENV}_gold"


@dp.materialized_view(
    name=f"{GOLD}.sales_by_store_hour",
    comment="Net sales per store, hour, category and tender. Voids excluded. Row filter: country managers.",
    cluster_by=["txn_hour_utc", "store_id"],
    row_filter=f"ROW FILTER {CAT}.security.rf_country ON (country)",
)
def sales_by_store_hour():
    s = spark.read.table(SILVER)
    return (s.where(~F.col("is_void"))
            .groupBy("country", "store_id", "business_date",
                     F.date_trunc("hour", "txn_ts_local").alias("txn_hour_local"),
                     F.date_trunc("hour", "txn_ts_utc").alias("txn_hour_utc"),
                     "category", "tender", "currency")
            .agg(F.count("*").alias("lines"),
                 F.countDistinct("register_id", "txn_id").alias("transactions"),
                 F.sum("quantity").alias("units"),
                 F.sum("net_amount").alias("net_sales_local"),
                 F.sum("net_amount_usd").alias("net_sales_usd"),
                 F.sum(F.when(F.col("status") == "RETURN", F.col("net_amount"))).alias("returns_local")))


@dp.materialized_view(
    name=f"{GOLD}.customer_daily",
    comment="Per-loyalty-member daily activity (feature source for churn). loyalty_id masked for non-PII readers.",
    cluster_by=["business_date"],
    schema=f"""
        country STRING, loyalty_id STRING MASK {CAT}.security.mask_pii, business_date DATE,
        transactions BIGINT, lines BIGINT, units BIGINT, spend_local DECIMAL(24,2), spend_usd DECIMAL(26,4),
        categories BIGINT, card_share DOUBLE, ewallet_share DOUBLE
    """,
)
def customer_daily():
    s = spark.read.table(SILVER).where(~F.col("is_void") & F.col("loyalty_id").isNotNull())
    return (s.groupBy("country", "loyalty_id", "business_date")
            .agg(F.countDistinct("store_id", "register_id", "txn_id").alias("transactions"),
                 F.count("*").alias("lines"),
                 F.sum("quantity").cast("bigint").alias("units"),
                 F.sum("net_amount").cast("decimal(24,2)").alias("spend_local"),
                 F.sum("net_amount_usd").cast("decimal(26,4)").alias("spend_usd"),
                 F.countDistinct("category").alias("categories"),
                 F.avg((F.col("tender") == "CARD").cast("double")).alias("card_share"),
                 F.avg((F.col("tender") == "EWALLET").cast("double")).alias("ewallet_share")))


@dp.materialized_view(
    name=f"{GOLD}.data_as_of",
    comment="Freshness for BI: data is complete up to data_as_of (end of the newest ingested live window).",
)
def data_as_of():
    s = spark.read.table(SILVER)
    return (s.groupBy("country")
            .agg(F.max(F.when(F.col("_pull_kind") == "live", F.col("_window_end"))).alias("data_as_of"),
                 F.max("txn_ts_utc").alias("latest_sale_utc"),
                 F.max("_ingest_ts").alias("last_pull_ts"),
                 F.max("_landed_ts").alias("last_landed_ts"),
                 F.max("_bronze_ts").alias("last_bronze_ts"),
                 F.max("_silver_ts").alias("last_silver_ts"))
            .withColumn("gold_refreshed_at", F.current_timestamp())
            .withColumn("minutes_behind", F.round((F.unix_timestamp("gold_refreshed_at")
                                                   - F.unix_timestamp("data_as_of")) / 60, 1))
            .withColumn("status", F.when(F.col("minutes_behind") <= 15, "FRESH").otherwise("STALE")))


@dp.materialized_view(
    name=f"{GOLD}.pipeline_latency",
    comment="End-to-end latency per stage for live records pulled in the last 60 minutes (minutes).",
)
def pipeline_latency():
    s = (spark.read.table(SILVER)
         .where((F.col("_pull_kind") == "live") & (F.col("_ingest_ts") > F.current_timestamp() - F.expr("INTERVAL 60 MINUTES"))))
    now = F.current_timestamp()

    def mins(a, b):
        return (F.unix_timestamp(a) - F.unix_timestamp(b)) / 60

    stages = s.select(
        "country",
        mins("_landed_ts", "txn_ts_utc").alias("poll"),       # sale -> window closes -> pulled -> landed
        mins("_bronze_ts", "_landed_ts").alias("bronze"),     # landed -> picked up by Auto Loader
        mins("_silver_ts", "_bronze_ts").alias("silver"),     # bronze -> translated + merged
        mins(now, "_silver_ts").alias("gold"),                # silver -> this gold refresh
        mins(now, "txn_ts_utc").alias("end_to_end"),
    )
    aggs = []
    for c in ["poll", "bronze", "silver", "gold", "end_to_end"]:
        aggs += [F.round(F.percentile_approx(c, 0.5), 2).alias(f"{c}_p50"),
                 F.round(F.percentile_approx(c, 0.95), 2).alias(f"{c}_p95")]
    return stages.groupBy("country").agg(F.count("*").alias("records"), *aggs).withColumn("measured_at", now)
