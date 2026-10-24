"""Global layer: the only data that leaves a country, and only aggregated and PII-free.

Default design (demo): this pipeline reads each country's *gold aggregates* (never silver)
and publishes country x hour x category totals in USD to pos_global.

PROD: the per-country gold aggregates are published from each country's metastore with
Delta Sharing (Databricks-to-Databricks) to the global workspace, and this pipeline reads the
shared tables. Raw / silver data and PII never leave the country. Alternatives (direct Delta
Sharing of detail with row filters, Lakehouse Federation) are discussed in governance/README.md.
"""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F

ENV = spark.conf.get("pos.env")
COUNTRIES = spark.conf.get("pos.countries").split(",")
GOLD = f"pos_global.{ENV}_gold"


def _union(table: str):
    return reduce(lambda a, b: a.unionByName(b),
                  [spark.read.table(f"pos_{c}.{ENV}_gold.{table}") for c in COUNTRIES])


@dp.materialized_view(
    name=f"{GOLD}.sales_by_country_hour",
    comment="Global aggregated sales (USD). No store detail, no PII. Row filter: execs all, managers own country.",
    cluster_by=["txn_hour_utc", "country"],
    row_filter="ROW FILTER pos_global.security.rf_global ON (country)",
)
def sales_by_country_hour():
    return (_union("sales_by_store_hour")
            .groupBy("country", "business_date", "txn_hour_utc", "category")
            .agg(F.countDistinct("store_id").alias("active_stores"),
                 F.sum("transactions").alias("transactions"),
                 F.sum("units").alias("units"),
                 F.round(F.sum("net_sales_usd"), 2).alias("net_sales_usd")))


@dp.materialized_view(name=f"{GOLD}.data_as_of", comment="Freshness per country, for every BI page.")
def data_as_of():
    return _union("data_as_of").select("country", "data_as_of", "latest_sale_utc", "gold_refreshed_at",
                                       "minutes_behind", "status")


@dp.materialized_view(name=f"{GOLD}.pipeline_latency", comment="Latency per stage per country (minutes).")
def pipeline_latency():
    return _union("pipeline_latency")
