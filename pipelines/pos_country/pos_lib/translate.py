"""Legacy -> business translation for one country. Pure DataFrame functions so they run the
same in the Lakeflow pipeline and in local PySpark unit tests (tests/unit/test_translate.py).

The legacy system writes store-local timestamps with no offset, in a different format per
country, amounts with a decimal comma in DE, and coded tender/status fields.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

COUNTRY = {
    "PH": {"tz": "Asia/Manila", "dt": "dd/MM/yyyy HH:mm:ss", "decimal_comma": False, "currency": "PHP"},
    "SG": {"tz": "Asia/Singapore", "dt": "yyyyMMddHHmmss", "decimal_comma": False, "currency": "SGD"},
    "DE": {"tz": "Europe/Berlin", "dt": "dd.MM.yyyy HH:mm:ss", "decimal_comma": True, "currency": "EUR"},
    "US": {"tz": "America/New_York", "dt": "MM/dd/yy hh:mm:ss a", "decimal_comma": False, "currency": "USD"},
}
TENDER = {"01": "CASH", "02": "CARD", "03": "EWALLET", "04": "GIFT_CARD"}
STATUS = {"S": "SALE", "R": "RETURN", "V": "VOID"}
FX_TO_USD = {"PHP": 0.0175, "SGD": 0.74, "EUR": 1.08, "USD": 1.0}
CATEGORIES = ["GROCERY", "BEVERAGE", "SNACKS", "DAIRY", "BAKERY", "FROZEN",
              "HOUSEHOLD", "PERSONAL_CARE", "ELECTRONICS", "APPAREL", "TOYS", "PHARMACY"]

KEYS = ["store_id", "register_id", "txn_id", "line_no"]

# Silver data-quality rules. A row must pass all of them; failures go to quarantine with the
# names of the rules it broke.
QUALITY_RULES = {
    "valid_shape": "_field_count = 16",
    "has_store": "store_id IS NOT NULL AND store_id <> ''",
    "has_keys": "register_id IS NOT NULL AND txn_id IS NOT NULL AND line_no IS NOT NULL",
    "valid_txn_ts": "txn_ts_utc IS NOT NULL AND txn_ts_utc > TIMESTAMP'2000-01-01'",
    "valid_qty": "quantity IS NOT NULL AND quantity <> 0",
    "valid_price": "unit_price IS NOT NULL AND unit_price >= 0",
    "known_tender": "tender IS NOT NULL",
    "known_status": "status IS NOT NULL",
}


def _map(col: str, mapping: dict) -> Column:
    # try_element_at: unknown legacy codes become NULL (caught by QUALITY_RULES) instead of
    # raising under ANSI mode.
    m = F.create_map(*[F.lit(x) for kv in mapping.items() for x in kv])
    return F.try_element_at(m, F.col(col))


def _try_cast(sql: str, typ: str) -> Column:
    return F.expr(f"try_cast({sql} AS {typ})")


def _amount(col: str, decimal_comma: bool) -> Column:
    return _try_cast(f"replace({col}, ',', '.')" if decimal_comma else col, "DECIMAL(12,2)")


def translate(df: DataFrame, country: str) -> DataFrame:
    """bronze columns (all strings + lineage) -> typed silver columns. Never raises on bad data:
    unparseable values become NULL and are caught by QUALITY_RULES."""
    cfg = COUNTRY[country]
    fx = FX_TO_USD[cfg["currency"]]
    ts_local = F.try_to_timestamp(F.col("TXN_DT"), F.lit(cfg["dt"]))
    sku_n = _try_cast("regexp_extract(SKU, '(\\\\d+)', 1)", "INT")
    out = df.select(
        F.col("RGN").alias("country"),
        F.expr("nullif(STR, '')").alias("store_id"),
        F.col("REG").alias("register_id"),
        F.col("TXN").alias("txn_id"),
        _try_cast("LN", "INT").alias("line_no"),
        ts_local.alias("txn_ts_local"),
        F.to_utc_timestamp(ts_local, cfg["tz"]).alias("txn_ts_utc"),
        F.to_date(ts_local).alias("business_date"),
        F.col("SKU").alias("sku"),
        F.when(sku_n.isNotNull(), F.element_at(F.array(*[F.lit(c) for c in CATEGORIES]), sku_n % 12 + 1)).alias("category"),
        _try_cast("QTY", "INT").alias("quantity"),
        _amount("UNIT_PRC", cfg["decimal_comma"]).alias("unit_price"),
        _amount("DISC", cfg["decimal_comma"]).alias("discount"),
        F.col("CURR").alias("currency"),
        _map("TND", TENDER).alias("tender"),
        _map("STAT", STATUS).alias("status"),
        F.expr("nullif(LYL, '')").alias("loyalty_id"),
        F.expr("nullif(EMAIL, '')").alias("customer_email"),
        _try_cast("UPD_SEQ", "INT").alias("update_seq"),
        "_record_hash", "_field_count", "_ingest_ts", "_pull_id", "_pull_kind",
        "_window_start", "_window_end", "_landed_ts", "_bronze_ts", "_raw_line",
    )
    net = (F.col("quantity") * (F.col("unit_price") - F.coalesce(F.col("discount"), F.lit(0)))).cast("decimal(14,2)")
    return (out.withColumn("net_amount", net)
               .withColumn("net_amount_usd", (F.col("net_amount") * F.lit(fx)).cast("decimal(16,4)"))
               .withColumn("is_void", F.col("status") == "VOID"))


def all_rules_pass() -> str:
    return " AND ".join(f"coalesce(({r}), false)" for r in QUALITY_RULES.values())


def failed_rules() -> Column:
    return F.filter(
        F.array(*[F.when(~F.coalesce(F.expr(r), F.lit(False)), F.lit(n)) for n, r in QUALITY_RULES.items()]),
        lambda x: x.isNotNull(),
    )


SILVER_COLUMNS = [
    "country", "store_id", "register_id", "txn_id", "line_no", "txn_ts_local", "txn_ts_utc", "business_date",
    "sku", "category", "quantity", "unit_price", "discount", "net_amount", "net_amount_usd", "currency",
    "tender", "status", "is_void", "loyalty_id", "customer_email", "update_seq",
    "_record_hash", "_ingest_ts", "_pull_id", "_pull_kind", "_window_start", "_window_end",
    "_landed_ts", "_bronze_ts",
]
