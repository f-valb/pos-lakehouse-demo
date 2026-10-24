import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ml"))
pytest.importorskip("mlflow")

from ml_lib import features, forecast, history, registry  # noqa: E402

TODAY = date(2026, 10, 24)


def test_history_has_churners_with_decaying_activity():
    df = history.customer_daily_history("SG", TODAY)
    assert df["loyalty_id"].str.match(r"^SGL\d{7}$").all()
    last = df.groupby("loyalty_id")["business_date"].max()
    gone = (last < TODAY - timedelta(days=30)).mean()
    assert 0.1 < gone < 0.5  # a realistic share of customers have stopped buying


def test_psi_flags_shift_and_not_noise():
    rng = np.random.default_rng(0)
    base = rng.lognormal(3, 0.5, 20000)
    edges = features.bin_edges(base)
    exp = features.proportions(base, edges)
    same = features.proportions(rng.lognormal(3, 0.5, 20000), edges)
    shifted = features.proportions(rng.lognormal(3, 0.5, 20000) * 3, edges)
    assert features.psi(exp, same) < 0.05
    assert features.psi(exp, shifted) > features.PSI_ALERT


def test_many_models_fit_and_route():
    pdf = history.store_category_daily_history("SG", TODAY)
    fits = pd.concat([forecast.fit_group(g) for _, g in pdf.groupby(["store_id", "category"])])
    assert len(fits) == 20 * 12
    assert forecast.overall_wape(fits) < 0.2

    class Ctx:
        artifacts = {}

    p = Path("/tmp/_fc_params.parquet")
    fits.to_parquet(p)
    Ctx.artifacts = {"params": str(p)}
    m = forecast.ManyModelsForecaster()
    m.load_context(Ctx)
    out = m.predict(None, pd.DataFrame({"store_id": ["SG0001", "XX9999"], "category": ["GROCERY", "GROCERY"],
                                        "business_date": [TODAY + timedelta(days=3)] * 2}))
    assert out.iloc[0] > 0 and np.isnan(out.iloc[1])


@pytest.mark.parametrize("chal,champ,ok", [(0.80, None, True), (0.80, 0.79, True), (0.795, 0.80, True),
                                           (0.75, 0.80, False), (0.55, None, False)])
def test_gate_auc(chal, champ, ok):
    assert registry.gate(chal, champ, higher_is_better=True, tolerance=0.01, floor=0.6)[0] is ok


def test_gate_wape_lower_is_better():
    assert registry.gate(0.10, 0.11, higher_is_better=False, tolerance=0.02, floor=0.5)[0]
    assert not registry.gate(0.15, 0.10, higher_is_better=False, tolerance=0.02, floor=0.5)[0]


def test_customer_profiles_match_region_config():
    from common.customers import CUSTOMERS
    from common.regions import FX_TO_USD, REGIONS

    assert CUSTOMERS == {cc: r.customers for cc, r in REGIONS.items()}
    assert history.COUNTRIES.keys() == REGIONS.keys()
    assert {cc: v[2] for cc, v in history.COUNTRIES.items()} == {cc: FX_TO_USD[r.currency] for cc, r in REGIONS.items()}
