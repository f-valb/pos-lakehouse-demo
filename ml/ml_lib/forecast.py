"""Demand forecast, many-models pattern: one small model per (store, category), fitted in
parallel with applyInPandas, packaged as ONE MLflow pyfunc that routes each row to its model.

Per-group model: linear trend + day-of-week effects, least squares on the last 120 days.
Deliberately simple (no extra dependencies, fits in milliseconds); the pattern is the point,
and any per-group estimator (Prophet, ETS, LightGBM) drops into `fit_group` unchanged.
"""

from __future__ import annotations

import json
from datetime import date

import mlflow.pyfunc
import numpy as np
import pandas as pd

TRAIN_DAYS = 120
BACKTEST_DAYS = 14
FIT_SCHEMA = "store_id string, category string, t0 string, coef string, n_obs int, backtest_wape double, backtest_units double"


def _design(t: np.ndarray, dow: np.ndarray) -> np.ndarray:
    x = np.zeros((len(t), 8))
    x[:, 0] = 1
    x[:, 1] = t
    for k in range(1, 7):
        x[:, 1 + k] = (dow == k).astype(float)
    return x


def _fit(t, dow, y):
    coef, *_ = np.linalg.lstsq(_design(t, dow), y, rcond=None)
    return coef


def _predict(coef, t, dow):
    return np.clip(_design(t, dow) @ np.asarray(coef), 0, None)


def fit_group(pdf: pd.DataFrame) -> pd.DataFrame:
    """applyInPandas function: pdf = daily units for one (store_id, category)."""
    pdf = pdf.sort_values("business_date").tail(TRAIN_DAYS)
    d = pd.to_datetime(pdf["business_date"])
    t0 = d.min()
    t = (d - t0).dt.days.to_numpy(float)
    dow = d.dt.weekday.to_numpy()
    y = pdf["units"].to_numpy(float)
    wape, bt_units = np.nan, 0.0
    if len(y) > BACKTEST_DAYS + 28:
        cut = len(y) - BACKTEST_DAYS
        c = _fit(t[:cut], dow[:cut], y[:cut])
        yhat = _predict(c, t[cut:], dow[cut:])
        bt_units = float(y[cut:].sum())
        wape = float(np.abs(y[cut:] - yhat).sum() / max(bt_units, 1))
    coef = _fit(t, dow, y)
    return pd.DataFrame([{
        "store_id": pdf["store_id"].iloc[0], "category": pdf["category"].iloc[0], "t0": t0.date().isoformat(),
        "coef": json.dumps([round(float(v), 6) for v in coef]), "n_obs": int(len(y)),
        "backtest_wape": wape, "backtest_units": bt_units,
    }])


class ManyModelsForecaster(mlflow.pyfunc.PythonModel):
    """Input: store_id, category, business_date. Output: forecast_units (NaN if the group is unknown)."""

    def load_context(self, context):
        p = pd.read_parquet(context.artifacts["params"])
        self.models = {(r.store_id, r.category): (date.fromisoformat(r.t0), json.loads(r.coef)) for r in p.itertuples()}

    def predict(self, context, model_input: pd.DataFrame, params=None) -> pd.Series:
        out = np.full(len(model_input), np.nan)
        dates = pd.to_datetime(model_input["business_date"])
        for i, (s, c, d) in enumerate(zip(model_input["store_id"], model_input["category"], dates, strict=True)):
            m = self.models.get((s, c))
            if m:
                t = np.array([(d.date() - m[0]).days], float)
                out[i] = _predict(m[1], t, np.array([d.weekday()]))[0]
        return pd.Series(out, name="forecast_units")


def overall_wape(fits: pd.DataFrame) -> float:
    f = fits.dropna(subset=["backtest_wape"])
    return float((f["backtest_wape"] * f["backtest_units"]).sum() / max(f["backtest_units"].sum(), 1))
