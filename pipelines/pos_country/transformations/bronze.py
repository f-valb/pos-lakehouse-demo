"""Bronze: raw, append-only. Every pulled version of every record is kept (re-pulls, backfills,
the original live pull), exactly as the legacy API returned it, plus lineage.

Auto Loader in directory-listing mode over the UC landing volume. PROD: the landing location is
an external volume / external location on the country's S3 bucket with
`cloudFiles.useManagedFileEvents = true` (file notification mode) so discovery cost does not
grow with the number of files.
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CC = spark.conf.get("pos.country")
ENV = spark.conf.get("pos.env")
CAT = f"pos_{CC.lower()}"
LANDING = spark.conf.get("pos.landing_path")


@dp.table(
    name=f"{CAT}.{ENV}_bronze.pos_lines_raw",
    comment="Raw legacy POS lines, one row per line per pull. Append-only.",
    cluster_by=["_window_start"],
    # Bronze is the only copy of what the API returned: a full refresh must never wipe it in prod.
    table_properties={"delta.enableRowTracking": "true",
                      "pipelines.reset.allowed": "false" if ENV == "prod" else "true"},
)
def pos_lines_raw():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.partitionColumns", "")  # region=/date=/window= are in the data already
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(LANDING)
        .withColumn("_landed_ts", F.col("_metadata.file_modification_time"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_bronze_ts", F.current_timestamp())
    )
