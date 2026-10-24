"""Silver translation + quality rules on real generator output, in local Spark (ANSI on)."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from pos_lib import translate as tr

from common import regions
from ingestion.legacy_parser import to_table
from ingestion.models import PullTask, iso
from mock_pos_api.generator import window_lines

pytestmark = pytest.mark.spark
T0 = datetime(2026, 10, 20, 11, 0, tzinfo=UTC)


def bronze_df(spark, region, lines=None):
    task = PullTask(region, iso(T0), iso(T0 + timedelta(minutes=2)), "live", iso(T0 + timedelta(minutes=3)),
                    iso(T0 + timedelta(minutes=3)))
    lines = lines or window_lines(region, T0, T0 + timedelta(minutes=2), T0 + timedelta(hours=1), 20000)
    t = to_table(lines, task)
    now = pa.array([T0 + timedelta(minutes=4)] * t.num_rows, pa.timestamp("us", tz="UTC"))
    t = t.append_column("_landed_ts", now).append_column("_bronze_ts", now)
    return spark.createDataFrame(t.to_pandas())


def test_pipeline_constants_match_generator():
    for cc, r in regions.REGIONS.items():
        assert tr.COUNTRY[cc]["tz"] == r.tz and tr.COUNTRY[cc]["currency"] == r.currency
        assert tr.COUNTRY[cc]["decimal_comma"] == r.decimal_comma
    assert tr.TENDER == regions.TENDER_CODES and tr.STATUS == regions.STATUS_CODES
    assert tr.FX_TO_USD == regions.FX_TO_USD


@pytest.mark.parametrize("cc", ["PH", "SG", "DE", "US"])
def test_every_country_format_parses(spark, cc):
    df = tr.translate(bronze_df(spark, cc), cc)
    good = df.where(tr.all_rules_pass())
    total, ok = df.count(), good.count()
    assert ok / total > 0.98
    row = good.orderBy("txn_ts_utc").first()
    # Sale time, converted from store-local legacy format back to UTC, is inside the window.
    assert T0.replace(tzinfo=None) <= row.txn_ts_utc < (T0 + timedelta(minutes=2)).replace(tzinfo=None)
    assert row.unit_price is not None and row.tender in tr.TENDER.values() and row.category in tr.CATEGORIES


def test_bad_rows_fail_named_rules(spark):
    base = window_lines("DE", T0, T0 + timedelta(minutes=1), T0 + timedelta(hours=1), 20000)[0]
    f = base.split("|")
    bad = {
        "has_store": f[:1] + [""] + f[2:],
        "valid_qty": f[:7] + ["1O"] + f[8:],
        "valid_txn_ts": f[:5] + ["00/00/0000 00:00:00"] + f[6:],
        "known_tender": f[:11] + ["99"] + f[12:],
        "valid_price": f[:8] + ["-" + f[8]] + f[9:],
    }
    lines = ["|".join(v) for v in bad.values()] + ["DE|only|four|fields"]
    df = tr.translate(bronze_df(spark, "DE", lines), "DE").withColumn("failed", tr.failed_rules())
    got = {tuple(r.failed) for r in df.collect()}
    for rule in bad:
        assert any(rule in g for g in got), rule
    assert any("valid_shape" in g for g in got)
    assert df.where(tr.all_rules_pass()).count() == 0


def test_de_decimal_comma_and_void(spark):
    line = "DE|DE0001|01|000000001|001|20.10.2026 13:00:00|SKU00013|2|12,50|1,25|EUR|02|V|DEL0000001|a@b.de|2"
    r = tr.translate(bronze_df(spark, "DE", [line]), "DE").first()
    assert str(r.unit_price) == "12.50" and str(r.net_amount) == "22.50"
    assert r.status == "VOID" and r.is_void and r.update_seq == 2
    assert r.txn_ts_utc == datetime(2026, 10, 20, 11, 0)  # CEST = UTC+2
    assert r.category == tr.CATEGORIES[13 % 12]


def test_us_ampm_format(spark):
    line = "US|US0001|01|000000001|001|10/20/26 07:05:09 PM|SKU00001|1|3.00|0.00|USD|01|S|||1"
    r = tr.translate(bronze_df(spark, "US", [line]), "US").first()
    assert r.txn_ts_local == datetime(2026, 10, 20, 19, 5, 9)
    assert r.txn_ts_utc == datetime(2026, 10, 20, 23, 5, 9)  # EDT = UTC-4
