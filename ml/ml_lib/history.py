"""SIMULATED history for ML. The live demo stream only covers the last few hours, which is not
enough for churn labels or weekly seasonality, so we seed 180 days of per-customer and
per-store activity that is consistent with the live generator (same loyalty-ID format, same
stores and categories, same currencies).

Churn signal: ~25% of customers churn at a random date; their visit rate decays over the
preceding 4-6 weeks and they drift away from card payments. That is what the model has to find.
Customer behaviour comes from common/customers.py, shared with the live generator.
"""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from common.customers import HISTORY_DAYS, STORE_WEEKEND_FACTOR, WEEKEND_FACTOR, profiles

COUNTRIES = {  # customers, stores, currency, fx_to_usd (matches common/regions.py)
    "PH": (20000, 40, 0.0175), "SG": (10000, 20, 0.74), "DE": (15000, 30, 1.08), "US": (30000, 60, 1.0),
}
CATEGORIES = ["GROCERY", "BEVERAGE", "SNACKS", "DAIRY", "BAKERY", "FROZEN",
              "HOUSEHOLD", "PERSONAL_CARE", "ELECTRONICS", "APPAREL", "TOYS", "PHARMACY"]
DAYS = HISTORY_DAYS


def customer_daily_history(cc: str, today: date, seed: int = 7) -> pd.DataFrame:
    """Per-customer daily activity for the HISTORY_DAYS before `today`, from the shared profiles
    (common/customers.py) that the live generator also uses."""
    _, _, fx = COUNTRIES[cc]
    prof = profiles(cc)
    rng = np.random.default_rng([seed, ord(cc[0]), ord(cc[1])])
    start = today - timedelta(days=DAYS)
    day_idx = np.arange(DAYS)
    rate = np.stack([prof.activity(int(d)) for d in day_idx], axis=1)  # [customer, day]
    dow = np.array([(start + timedelta(days=int(d))).weekday() for d in day_idx])
    rate = rate * np.where(dow >= 5, WEEKEND_FACTOR, 1.0)[None, :]

    visits = rng.poisson(rate)
    c_idx, d_idx = np.nonzero(visits)
    tx = visits[c_idx, d_idx]
    spend_usd = np.round(prof.basket_usd[c_idx] * tx * rng.lognormal(0, 0.35, len(tx)), 2)
    lines = tx * rng.integers(1, 5, len(tx))
    decay = np.clip((prof.churn_day[c_idx] - d_idx) / prof.decay_len[c_idx], 0, 1)
    pre_churn = (1 - decay) * prof.churner[c_idx]  # churners drift away from card payments as they leave
    return pd.DataFrame({
        "country": cc,
        "loyalty_id": [f"{cc}L{c:07d}" for c in c_idx],
        "business_date": [start + timedelta(days=int(d)) for d in d_idx],
        "transactions": tx.astype("int64"),
        "lines": lines.astype("int64"),
        "units": (lines * rng.integers(1, 3, len(tx))).astype("int64"),
        "spend_local": np.round(spend_usd / fx, 2),
        "spend_usd": spend_usd,
        "categories": np.minimum(lines, rng.integers(1, 6, len(tx))).astype("int64"),
        "card_share": np.clip(prof.card_pref[c_idx] - 0.25 * pre_churn + rng.normal(0, 0.05, len(tx)), 0, 1),
        "ewallet_share": np.clip(prof.ewallet_pref[c_idx] + rng.normal(0, 0.03, len(tx)), 0, 1),
    })


def store_category_daily_history(cc: str, today: date, records_per_minute: int = 50000, seed: int = 11) -> pd.DataFrame:
    """Daily units per store x category at the SAME scale as the live generator (so forecasts and
    live actuals are comparable): lines/day = rpm x traffic share x 1440 (the traffic curve averages
    1.0), spread uniformly over stores and the 12 categories, ~2 units per line, weekend uplift."""
    from common.regions import REGIONS

    _, n_stores, fx = COUNTRIES[cc]
    rng = np.random.default_rng([seed, ord(cc[0]), ord(cc[1])])
    start = today - timedelta(days=DAYS)
    base_units = records_per_minute * REGIONS[cc].traffic_share * 1440 / n_stores / len(CATEGORIES) * 2.0
    rows = []
    for s in range(1, n_stores + 1):
        for cat in CATEGORIES:
            trend = rng.normal(0.0005, 0.0008)  # slow growth / decline per day
            price_usd = rng.uniform(4, 12)
            for d in range(DAYS):
                day = start + timedelta(days=d)
                mu = base_units * (1 + trend * (d - DAYS)) * (STORE_WEEKEND_FACTOR if day.weekday() >= 5 else 1.0)
                units = max(0, int(rng.normal(mu, 0.05 * mu)))
                rows.append((cc, f"{cc}{s:04d}", cat, day, units, round(units * price_usd / fx, 2)))
    return pd.DataFrame(rows, columns=["country", "store_id", "category", "business_date", "units", "net_sales_local"])
