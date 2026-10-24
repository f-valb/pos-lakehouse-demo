"""Deterministic synthetic POS line-item generator in the legacy pipe-delimited format.

Determinism matters: the same (region, minute) always yields the same base records, so a
retried pull of a historical window returns identical data (which lets us prove the poller is
idempotent). Realism comes from what *does* change over time, as in a real POS system:

* Late voids: ~2% of transactions are voided 3-20 min after the sale. A pull after the void
  sees STAT=V and UPD_SEQ=2 for the same business key -> silver must keep the newest version.
* Legacy bugs: ~0.5% of lines are emitted twice in one response; ~0.3% are malformed
  (missing store, garbage qty, impossible date, unknown tender code, negative price).
* Traffic follows a per-country local-time curve (lunch and evening peaks).
* Loyalty customers buy at the rate of their shared profile (common/customers.py), the same
  profiles the ML history uses, so churned customers never appear in the live stream. The
  loyalty share of transactions is therefore small in the demo; with a realistic member base
  (millions) it would be ~60%.
"""

from __future__ import annotations

import hashlib
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from zoneinfo import ZoneInfo

import numpy as np
from faker import Faker

from common.customers import HISTORY_DAYS, STORE_WEEKEND_FACTOR, WEEKEND_FACTOR, profiles
from common.regions import LEGACY_COLUMNS, REGIONS, Region

N_SKUS = 500
AVG_LINES_PER_TXN = 2.5
VOID_RATE = 0.02
BAD_ROW_RATE = 0.003
DUP_LINE_RATE = 0.005
EXPOSE_DELAY_S = 20  # the legacy system only exposes sales once they are ~20s old

TENDER_MIX = {  # probability of codes 01..04 per country
    "PH": [0.45, 0.25, 0.27, 0.03],
    "SG": [0.10, 0.50, 0.37, 0.03],
    "DE": [0.40, 0.55, 0.02, 0.03],
    "US": [0.15, 0.75, 0.05, 0.05],
}
DEFECTS = ["missing_store", "bad_qty", "bad_date", "bad_tender", "neg_price"]


def _seed(*parts: object) -> int:
    h = hashlib.sha256("|".join(map(str, parts)).encode()).digest()
    return int.from_bytes(h[:8], "little")


def _raw_curve(h: float) -> float:
    daytime = 1 / (1 + math.exp(-(h - 8.5) * 2)) * (1 / (1 + math.exp((h - 22) * 2)))
    return 0.15 + 0.5 * daytime + 0.9 * math.exp(-(((h - 12.5) / 1.3) ** 2)) + 1.1 * math.exp(-(((h - 19) / 1.8) ** 2))


_CURVE_NORM = sum(_raw_curve(m / 60) for m in range(1440)) / 1440


def traffic_multiplier(local_hour: float) -> float:
    """Normalised so the daily average is 1.0 (peak is ~2.3x, night ~0.2x)."""
    return _raw_curve(local_hour) / _CURVE_NORM


@dataclass(frozen=True)
class _Catalog:
    sku_price: np.ndarray  # local-currency unit price per SKU
    first_names: list[str]
    last_names: list[str]
    tld: str


@lru_cache(maxsize=8)
def _catalog(region_code: str) -> _Catalog:
    r = REGIONS[region_code]
    rng = np.random.default_rng(_seed("catalog", region_code))
    from common.regions import FX_TO_USD

    usd = np.round(np.exp(rng.normal(1.6, 0.9, N_SKUS)), 2).clip(0.5, 400)
    local = np.round(usd / FX_TO_USD[r.currency], 2)
    fake = Faker({"PH": "en_PH", "SG": "en_US", "DE": "de_DE", "US": "en_US"}[region_code])
    fake.seed_instance(_seed("names", region_code) % (2**31))
    first = [fake.first_name().lower().replace(" ", "") for _ in range(300)]
    last = [fake.last_name().lower().replace(" ", "") for _ in range(300)]
    return _Catalog(local, first, last, {"PH": "ph", "SG": "sg", "DE": "de", "US": "com"}[region_code])


@dataclass(frozen=True)
class LegacyLine:
    txn_epoch: float  # true UTC epoch of the sale (used for windowing)
    head: str  # RGN..TND
    stat: str  # status at sale time (S or R)
    tail: str  # LYL|EMAIL
    void_at: float | None  # epoch when this transaction gets voided, if ever
    dup: bool  # legacy bug: line emitted twice

    def render(self, now_epoch: float) -> str:
        voided = self.void_at is not None and now_epoch >= self.void_at
        stat, seq = ("V", "2") if voided else (self.stat, "1")
        return f"{self.head}|{stat}|{self.tail}|{seq}"


def lines_per_minute(region: Region, minute_start_utc: datetime, total_rpm: int) -> int:
    local = minute_start_utc.astimezone(ZoneInfo(region.tz))
    mult = traffic_multiplier(local.hour + local.minute / 60) * (STORE_WEEKEND_FACTOR if local.weekday() >= 5 else 1.0)
    return max(1, int(total_rpm * region.traffic_share * mult))


def _fmt_amount(v: float, decimal_comma: bool) -> str:
    s = f"{v:.2f}"
    return s.replace(".", ",") if decimal_comma else s


@lru_cache(maxsize=512)
def generate_minute(region_code: str, minute_epoch: int, total_rpm: int) -> tuple[LegacyLine, ...]:
    """All line items whose sale happened in [minute_epoch, minute_epoch + 60s) for one country."""
    r = REGIONS[region_code]
    cat = _catalog(region_code)
    tz = ZoneInfo(r.tz)
    minute_start = datetime.fromtimestamp(minute_epoch, UTC)
    rng = np.random.default_rng(_seed("minute", region_code, minute_epoch))

    n_target = lines_per_minute(r, minute_start, total_rpm)
    n_txn = max(1, int(n_target / AVG_LINES_PER_TXN))
    stores = rng.integers(1, r.stores + 1, n_txn)
    regs = rng.integers(1, r.registers_per_store + 1, n_txn)
    secs = np.sort(rng.uniform(0, 60, n_txn))
    n_lines = rng.integers(1, 5, n_txn)
    tender = rng.choice(["01", "02", "03", "04"], n_txn, p=TENDER_MIX[region_code])
    # Loyalty: expected visits this minute per customer = today's profile rate spread over the day
    # along the same traffic curve. Today is day HISTORY_DAYS of the shared profiles.
    prof = profiles(region_code)
    local = minute_start.astimezone(tz)
    per_min = prof.activity(HISTORY_DAYS) * (WEEKEND_FACTOR if local.weekday() >= 5 else 1.0) / 1440
    per_min = per_min * traffic_multiplier(local.hour + local.minute / 60)
    k = min(int(rng.poisson(per_min.sum())), n_txn)
    has_loyalty = np.zeros(n_txn, bool)
    cust = np.zeros(n_txn, int)
    if k:
        slots = rng.choice(n_txn, k, replace=False)
        has_loyalty[slots] = True
        cust[slots] = rng.choice(len(per_min), k, p=per_min / per_min.sum())
        u = rng.random(k)
        cp, ep = prof.card_pref[cust[slots]], prof.ewallet_pref[cust[slots]]
        tender[slots] = np.where(u < cp, "02", np.where(u < cp + (1 - cp) * ep, "03", "01"))
    is_return = rng.random(n_txn) < 0.03
    voided = rng.random(n_txn) < VOID_RATE
    void_delay = rng.integers(180, 1200, n_txn)

    out: list[LegacyLine] = []
    minute_idx = minute_epoch // 60
    for t in range(n_txn):
        ts = minute_epoch + float(secs[t])
        local_dt = datetime.fromtimestamp(ts, UTC).astimezone(tz)
        dt_str = local_dt.strftime(r.legacy_dt_format)
        store = f"{region_code}{stores[t]:04d}"
        reg = f"{regs[t]:02d}"
        txn = f"{minute_idx:09d}{t:05d}"
        if has_loyalty[t]:
            c = int(cust[t])
            lyl = f"{region_code}L{c:07d}"
            email = f"{cat.first_names[c % 300]}.{cat.last_names[(c * 7) % 300]}{c % 97}@example.{cat.tld}"
        else:
            lyl, email = "", ""
        stat = "R" if is_return[t] else "S"
        void_at = ts + float(void_delay[t]) if voided[t] else None
        line_rng = np.random.default_rng(_seed("lines", region_code, minute_epoch, t))
        for ln in range(1, int(n_lines[t]) + 1):
            sku_n = int(line_rng.integers(0, N_SKUS))
            qty = int(line_rng.integers(1, 4)) * (-1 if is_return[t] else 1)
            price = float(cat.sku_price[sku_n])
            disc = round(price * 0.1, 2) if line_rng.random() < 0.1 else 0.0
            f = {
                "RGN": region_code, "STR": store, "REG": reg, "TXN": txn, "LN": f"{ln:03d}",
                "TXN_DT": dt_str, "SKU": f"SKU{sku_n:05d}", "QTY": str(qty),
                "UNIT_PRC": _fmt_amount(price, r.decimal_comma), "DISC": _fmt_amount(disc, r.decimal_comma),
                "CURR": r.currency, "TND": str(tender[t]),
            }
            if line_rng.random() < BAD_ROW_RATE:
                defect = DEFECTS[int(line_rng.integers(0, len(DEFECTS)))]
                if defect == "missing_store":
                    f["STR"] = ""
                elif defect == "bad_qty":
                    f["QTY"] = "1O"  # letter O, a classic legacy keying error
                elif defect == "bad_date":
                    f["TXN_DT"] = "00/00/0000 00:00:00"
                elif defect == "bad_tender":
                    f["TND"] = "99"
                else:
                    f["UNIT_PRC"] = "-" + f["UNIT_PRC"]
            head = "|".join(f[c] for c in LEGACY_COLUMNS[:12])
            out.append(LegacyLine(ts, head, stat, f"{lyl}|{email}", void_at, line_rng.random() < DUP_LINE_RATE))
    return tuple(out)


def window_lines(region_code: str, start: datetime, end: datetime, now: datetime, total_rpm: int) -> list[str]:
    """Render every line with sale time in [start, end) that the legacy system exposes at `now`."""
    visible_until = min(end.timestamp(), now.timestamp() - EXPOSE_DELAY_S)
    lines: list[str] = []
    m = int(start.timestamp()) // 60 * 60
    now_e = now.timestamp()
    while m < visible_until:
        for rec in generate_minute(region_code, m, total_rpm):
            if start.timestamp() <= rec.txn_epoch < visible_until:
                s = rec.render(now_e)
                lines.append(s)
                if rec.dup:
                    lines.append(s)
        m += 60
    return lines


def header() -> str:
    return "|".join(LEGACY_COLUMNS)

