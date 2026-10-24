"""Silver: translated, validated, deduplicated current state per line item.

bronze --translate--> pos_lines_translated (temp view)
   |-- rows failing any rule ---------------------------> pos_lines_quarantine (+ failed rule names)
   '-- rows passing (expect_all_or_drop, metrics) -----> pos_lines_clean (temp view)
         drop identical re-pulls by content hash (30 min, processing-time watermark)
         AUTO CDC on (store, register, txn, line) sequenced by (_ingest_ts, update_seq)
                                                       --> pos_lines (SCD1, PII masked)

Why both a hash dedupe and AUTO CDC: AUTO CDC alone is correct (the newest pull wins, so a late
void overwrites the sale), but every identical re-pull would still rewrite the row. The hash
drop removes those no-op upserts cheaply; anything it misses is still handled by AUTO CDC.
"""

from pos_lib.translate import KEYS, QUALITY_RULES, SILVER_COLUMNS, all_rules_pass, failed_rules, translate
from pyspark import pipelines as dp
from pyspark.sql import functions as F

CC = spark.conf.get("pos.country")
ENV = spark.conf.get("pos.env")
CAT = f"pos_{CC.lower()}"
BRONZE = f"{CAT}.{ENV}_bronze.pos_lines_raw"
SILVER = f"{CAT}.{ENV}_silver"


@dp.temporary_view(name="pos_lines_translated")
def pos_lines_translated():
    return translate(spark.readStream.table(BRONZE), CC)


@dp.table(
    name=f"{SILVER}.pos_lines_quarantine",
    comment="Rows that failed silver quality rules, with the rule names and the raw legacy line.",
    cluster_by=["_ingest_ts"],
)
def pos_lines_quarantine():
    return (spark.readStream.table("pos_lines_translated")
            .where(f"NOT ({all_rules_pass()})")
            .select(F.col("_raw_line"), failed_rules().alias("failed_rules"), "country", "store_id", "txn_id",
                    "line_no", "_pull_id", "_pull_kind", "_ingest_ts", "_window_start", "_bronze_ts",
                    F.current_timestamp().alias("_quarantined_ts")))


@dp.temporary_view(name="pos_lines_clean")
@dp.expect_all_or_drop(QUALITY_RULES)
def pos_lines_clean():
    return (spark.readStream.table("pos_lines_translated")
            .withWatermark("_bronze_ts", "30 minutes")
            .dropDuplicatesWithinWatermark(["_record_hash"])
            .withColumn("_silver_ts", F.current_timestamp())
            .select(*SILVER_COLUMNS, "_field_count", "_silver_ts"))


dp.create_streaming_table(
    name=f"{SILVER}.pos_lines",
    comment="Current state per POS line item (latest pull wins). PII columns are masked.",
    cluster_by=["business_date", "store_id"],
    table_properties={"delta.enableRowTracking": "true", "delta.enableChangeDataFeed": "true"},
    schema=f"""
        country STRING, store_id STRING, register_id STRING, txn_id STRING, line_no INT,
        txn_ts_local TIMESTAMP, txn_ts_utc TIMESTAMP, business_date DATE, sku STRING, category STRING,
        quantity INT, unit_price DECIMAL(12,2), discount DECIMAL(12,2), net_amount DECIMAL(14,2),
        net_amount_usd DECIMAL(16,4), currency STRING, tender STRING, status STRING, is_void BOOLEAN,
        loyalty_id STRING MASK {CAT}.security.mask_pii,
        customer_email STRING MASK {CAT}.security.mask_email,
        update_seq INT, _record_hash STRING, _ingest_ts TIMESTAMP, _pull_id STRING, _pull_kind STRING,
        _window_start TIMESTAMP, _window_end TIMESTAMP, _landed_ts TIMESTAMP, _bronze_ts TIMESTAMP,
        _silver_ts TIMESTAMP
    """,
)

dp.create_auto_cdc_flow(
    target=f"{SILVER}.pos_lines",
    source="pos_lines_clean",
    keys=KEYS,
    sequence_by=F.struct("_ingest_ts", "update_seq"),
    stored_as_scd_type=1,
    except_column_list=["_field_count"],
)
