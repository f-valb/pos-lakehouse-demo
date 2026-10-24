"""Churn features: ONE definition used by training, batch scoring and drift monitoring, so there
is no training/serving skew. Input is a customer-daily activity frame (gold customer_daily
unioned with the seeded history)."""

from __future__ import annotations

import numpy as np

FEATURES = [
    "recency_days", "tenure_days", "freq_30", "freq_90", "spend_30_usd", "spend_90_usd",
    "avg_basket_90_usd", "trend_30_vs_prior", "categories_avg_90", "card_share_90", "ewallet_share_90",
]
LABEL = "churned_30d"
CHURN_HORIZON_DAYS = 30


def feature_sql(daily_view: str, as_of: str, with_label: bool) -> str:
    """Spark SQL producing one row per loyalty_id active in the 90 days up to `as_of` (inclusive)."""
    label = f"""
      , CASE WHEN max(CASE WHEN business_date > DATE'{as_of}'
                            AND business_date <= date_add(DATE'{as_of}', {CHURN_HORIZON_DAYS})
                           THEN 1 ELSE 0 END) = 1 THEN 0 ELSE 1 END AS {LABEL}""" if with_label else ""
    return f"""
    WITH d AS (SELECT * FROM {daily_view} WHERE business_date <= date_add(DATE'{as_of}', {CHURN_HORIZON_DAYS}))
    SELECT loyalty_id, DATE'{as_of}' AS as_of_date, first(country) AS country,
      datediff(DATE'{as_of}', max(CASE WHEN business_date <= DATE'{as_of}' THEN business_date END)) AS recency_days,
      datediff(DATE'{as_of}', min(business_date)) AS tenure_days,
      sum(CASE WHEN business_date > date_sub(DATE'{as_of}', 30) AND business_date <= DATE'{as_of}' THEN transactions ELSE 0 END) AS freq_30,
      sum(CASE WHEN business_date > date_sub(DATE'{as_of}', 90) AND business_date <= DATE'{as_of}' THEN transactions ELSE 0 END) AS freq_90,
      sum(CASE WHEN business_date > date_sub(DATE'{as_of}', 30) AND business_date <= DATE'{as_of}' THEN spend_usd ELSE 0 END) AS spend_30_usd,
      sum(CASE WHEN business_date > date_sub(DATE'{as_of}', 90) AND business_date <= DATE'{as_of}' THEN spend_usd ELSE 0 END) AS spend_90_usd,
      try_divide(sum(CASE WHEN business_date > date_sub(DATE'{as_of}', 90) AND business_date <= DATE'{as_of}' THEN spend_usd END),
                 sum(CASE WHEN business_date > date_sub(DATE'{as_of}', 90) AND business_date <= DATE'{as_of}' THEN transactions END)) AS avg_basket_90_usd,
      try_divide(sum(CASE WHEN business_date > date_sub(DATE'{as_of}', 30) AND business_date <= DATE'{as_of}' THEN transactions ELSE 0 END),
                 sum(CASE WHEN business_date > date_sub(DATE'{as_of}', 90) AND business_date <= date_sub(DATE'{as_of}', 30) THEN transactions ELSE 0 END) / 2.0 + 1) AS trend_30_vs_prior,
      avg(CASE WHEN business_date > date_sub(DATE'{as_of}', 90) AND business_date <= DATE'{as_of}' THEN categories END) AS categories_avg_90,
      avg(CASE WHEN business_date > date_sub(DATE'{as_of}', 90) AND business_date <= DATE'{as_of}' THEN card_share END) AS card_share_90,
      avg(CASE WHEN business_date > date_sub(DATE'{as_of}', 90) AND business_date <= DATE'{as_of}' THEN ewallet_share END) AS ewallet_share_90
      {label}
    FROM d
    GROUP BY loyalty_id
    HAVING freq_90 > 0
    """


# ---------------------------------------------------------------- drift (PSI)
def bin_edges(values: np.ndarray, n_bins: int = 10) -> list[float]:
    v = values[~np.isnan(values)]
    edges = np.unique(np.quantile(v, np.linspace(0, 1, n_bins + 1)))
    return edges[1:-1].tolist()  # interior edges; outer bins are open-ended


def proportions(values: np.ndarray, edges: list[float]) -> list[float]:
    v = values[~np.isnan(values)]
    idx = np.searchsorted(np.asarray(edges), v, side="right")
    counts = np.bincount(idx, minlength=len(edges) + 1).astype(float)
    return (counts / max(counts.sum(), 1)).tolist()


def psi(expected: list[float], actual: list[float], eps: float = 1e-4) -> float:
    """Population Stability Index. <0.1 stable, 0.1-0.25 moderate shift, >0.25 significant."""
    e = np.clip(np.asarray(expected), eps, None)
    a = np.clip(np.asarray(actual), eps, None)
    return float(np.sum((a - e) * np.log(a / e)))


PSI_ALERT = 0.25
# Features that trend with calendar time by construction (tenure grows by the gap between the
# training and scoring snapshots). Monitored and charted, but they do not raise drift alerts.
TIME_TRENDING = {"tenure_days"}
