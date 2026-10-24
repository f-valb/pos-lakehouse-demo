"""Hit the legacy API the naive way (N concurrent pulls, no backoff) to show it falling over.

    uv run python -m mock_pos_api.hammer --concurrency 30 --rounds 3
"""

from __future__ import annotations

import argparse
import asyncio
import os
import time
from collections import Counter
from datetime import UTC, datetime, timedelta

import httpx

from common.regions import REGIONS


async def one(client: httpx.AsyncClient, url: str, region: str, start: datetime) -> str:
    params = {"region": region, "from": start.isoformat(), "to": (start + timedelta(minutes=10)).isoformat()}
    try:
        r = await client.get(f"{url}/transactions", params=params, timeout=15)
        if r.status_code == 200 and "TRL|" not in r.text[-40:]:
            return "200-truncated"
        return str(r.status_code)
    except httpx.HTTPError as e:
        return type(e).__name__


async def main(url: str, concurrency: int, rounds: int) -> None:
    now = datetime.now(UTC).replace(second=0, microsecond=0)
    regions = list(REGIONS)
    async with httpx.AsyncClient() as client:
        for rnd in range(1, rounds + 1):
            t0 = time.time()
            tasks = [one(client, url, regions[i % 4], now - timedelta(minutes=15 + i)) for i in range(concurrency)]
            res = Counter(await asyncio.gather(*tasks))
            stats = (await client.get(f"{url}/admin/stats")).json()
            print(f"round {rnd}: {dict(res)}  in {time.time() - t0:.1f}s  "
                  f"api crashed={stats['crashed']} crashes={stats['crashes']} max_in_flight={stats['max_in_flight']}")
            await asyncio.sleep(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--url", default=os.environ.get("POS_API_URL", "http://127.0.0.1:8080"))
    p.add_argument("--concurrency", type=int, default=30)
    p.add_argument("--rounds", type=int, default=3)
    a = p.parse_args()
    asyncio.run(main(a.url, a.concurrency, a.rounds))
