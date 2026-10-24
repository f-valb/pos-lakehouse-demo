import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from mock_pos_api import app as api
from mock_pos_api.generator import DEFECTS, generate_minute, traffic_multiplier, window_lines

T0 = datetime(2026, 10, 20, 4, 0, tzinfo=UTC)  # 12:00 in Manila / Singapore


def test_window_is_deterministic():
    now = T0 + timedelta(hours=2)
    a = window_lines("PH", T0, T0 + timedelta(minutes=5), now, 20000)
    b = window_lines("PH", T0, T0 + timedelta(minutes=5), now, 20000)
    assert a == b and len(a) > 1000


def test_overlapping_windows_return_the_same_records():
    now = T0 + timedelta(hours=2)
    a = set(window_lines("SG", T0, T0 + timedelta(minutes=10), now, 20000))
    b = set(window_lines("SG", T0 + timedelta(minutes=5), T0 + timedelta(minutes=15), now, 20000))
    assert len(a & b) > 100  # overlap -> duplicates the lakehouse has to remove


def test_late_void_changes_status_and_update_seq():
    lines = generate_minute("US", int(T0.timestamp()), 20000)
    voided = [ln for ln in lines if ln.void_at is not None]
    assert voided, "expected some late voids"
    rec = voided[0]
    assert rec.render(rec.txn_epoch + 1).endswith("|1") and "|S|" in rec.render(rec.txn_epoch + 1)
    assert rec.render(rec.void_at + 1).endswith("|2") and "|V|" in rec.render(rec.void_at + 1)


def test_legacy_defects_and_duplicates_present():
    now = T0 + timedelta(hours=2)
    lines = window_lines("DE", T0, T0 + timedelta(minutes=30), now, 50000)
    dup_count = sum(c - 1 for c in Counter(lines).values() if c > 1)
    assert dup_count > 0
    bad = [ln for ln in lines if ln.split("|")[1] == "" or "1O" in ln.split("|")[7] or "00/00/0000" in ln]
    assert bad, f"expected some of {DEFECTS}"
    assert "," in lines[0].split("|")[8]  # DE uses decimal comma


def test_traffic_curve_has_peaks():
    assert traffic_multiplier(12.5) > 1.5 and traffic_multiplier(19) > 1.5
    assert traffic_multiplier(3) < 0.4


@pytest.fixture
def fresh_state(monkeypatch):
    s = api.ApiState(total_rpm=5000, soft_limit=2, hard_limit=4, crash_seconds=5)
    monkeypatch.setattr(api, "state", s)
    return s


async def _get(client, i):
    start = T0 + timedelta(minutes=i)
    r = await client.get("/transactions", params={"region": "PH", "from": start.isoformat(),
                                                  "to": (start + timedelta(minutes=5)).isoformat()})
    return r.status_code


def test_api_crashes_when_hammered(fresh_state):
    async def run():
        transport = httpx.ASGITransport(app=api.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            codes = await asyncio.gather(*[_get(c, i) for i in range(12)])
            after = await _get(c, 0)
        return codes, after

    codes, after = asyncio.run(run())
    assert 500 in codes
    assert fresh_state.crashes >= 1
    assert after == 500  # still down during the cooldown


def test_api_serves_sequential_requests_with_trailer(fresh_state):
    async def run():
        transport = httpx.ASGITransport(app=api.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            return [await c.get("/transactions", params={"region": "SG", "from": T0.isoformat(),
                                                         "to": (T0 + timedelta(minutes=2)).isoformat()})
                    for _ in range(3)]

    for r in asyncio.run(run()):
        assert r.status_code == 200
        last = r.text.strip().splitlines()[-1]
        assert last.startswith("TRL|")
        assert int(last.split("|")[1]) == len(r.text.strip().splitlines()) - 2


def test_region_outage_switch(fresh_state):
    fresh_state.failing_regions["SG"] = "error"

    async def run():
        transport = httpx.ASGITransport(app=api.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            sg = await c.get("/transactions", params={"region": "SG", "from": T0.isoformat(),
                                                      "to": (T0 + timedelta(minutes=1)).isoformat()})
            ph = await c.get("/transactions", params={"region": "PH", "from": T0.isoformat(),
                                                      "to": (T0 + timedelta(minutes=1)).isoformat()})
        return sg.status_code, ph.status_code

    assert asyncio.run(run()) == (503, 200)
