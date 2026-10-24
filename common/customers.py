"""Deterministic loyalty-customer profiles shared by the live generator and the ML history seed,
so a customer behaves the same in the live stream as in their simulated history (same visit
rate, same churn date, same tender preference). Without this, the live stream would make
every customer look active today and churn labels would be meaningless.

Day indexes are relative to HISTORY_DAYS before "today": history covers idx 0..179 and today
is idx 180. A churner with churn_day <= 180 buys nothing today.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from functools import lru_cache

import numpy as np

HISTORY_DAYS = 180
CUSTOMERS = {"PH": 20000, "SG": 10000, "DE": 15000, "US": 30000}  # == common.regions.REGIONS[cc].customers
WEEKEND_FACTOR = 1.35  # loyalty visits
STORE_WEEKEND_FACTOR = 1.25  # store traffic (live generator + store history)


def _seed(*parts) -> int:
    return int.from_bytes(hashlib.sha256("|".join(map(str, parts)).encode()).digest()[:8], "little")


@dataclass(frozen=True)
class Profiles:
    weekly: np.ndarray  # visits per week at full activity
    basket_usd: np.ndarray
    card_pref: np.ndarray
    ewallet_pref: np.ndarray
    joined: np.ndarray  # day idx the customer joined
    churner: np.ndarray
    churn_day: np.ndarray  # day idx the customer stops buying (huge for non-churners)
    decay_len: np.ndarray  # activity decays over this many days before churn

    def activity(self, day_idx: int | np.ndarray) -> np.ndarray:
        """Expected visits per day for every customer on day_idx (before weekend factor)."""
        d = np.asarray(day_idx)
        rate = self.weekly / 7
        joined = self.joined <= d
        decay = np.clip((self.churn_day - d) / self.decay_len, 0, 1)
        return np.where(joined, rate * np.where(self.churner, decay, 1.0), 0.0)


@lru_cache(maxsize=8)
def profiles(cc: str) -> Profiles:
    n = CUSTOMERS[cc]
    rng = np.random.default_rng(_seed("customer-profiles", cc))
    churner = rng.random(n) < 0.25
    return Profiles(
        weekly=rng.lognormal(0.0, 0.6, n).clip(0.2, 6),
        basket_usd=rng.gamma(3.0, 12.0, n),
        card_pref=rng.beta(2, 3, n),
        ewallet_pref=rng.beta(1.5, 5, n),
        joined=rng.integers(0, HISTORY_DAYS - 14, n),
        churner=churner,
        churn_day=np.where(churner, rng.integers(40, HISTORY_DAYS + 25, n), 10**6),
        decay_len=rng.integers(28, 45, n),
    )
