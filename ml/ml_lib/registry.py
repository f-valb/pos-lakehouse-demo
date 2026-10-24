"""Champion / challenger lifecycle on the Unity Catalog model registry.

new version --(alias)--> challenger --validation gate--> champion
                                            |                 '-- previous champion recorded as tag
                                            '-- fail: stays challenger, champion untouched
rollback = move the champion alias back to the recorded previous version (no retraining).
"""

from __future__ import annotations

from dataclasses import dataclass

from mlflow import MlflowClient
from mlflow.exceptions import MlflowException


@dataclass
class GateResult:
    promoted: bool
    reason: str
    challenger_version: str
    champion_version: str | None
    challenger_metric: float
    champion_metric: float | None


def champion_version(client: MlflowClient, name: str) -> str | None:
    try:
        return client.get_model_version_by_alias(name, "champion").version
    except MlflowException:
        return None


def gate(challenger: float, champion: float | None, higher_is_better: bool, tolerance: float, floor: float) -> tuple[bool, str]:
    """Pure decision function (unit-tested)."""
    ok_floor = challenger >= floor if higher_is_better else challenger <= floor
    if not ok_floor:
        return False, f"below absolute floor ({challenger:.4f} vs {floor})"
    if champion is None:
        return True, "no current champion"
    better = challenger >= champion - tolerance if higher_is_better else challenger <= champion * (1 + tolerance)
    return (better, f"challenger {challenger:.4f} vs champion {champion:.4f} (tolerance {tolerance})")


def promote_if_better(client: MlflowClient, name: str, version: str, challenger_metric: float,
                      champion_metric: float | None, higher_is_better: bool, tolerance: float, floor: float) -> GateResult:
    client.set_registered_model_alias(name, "challenger", version)
    current = champion_version(client, name)
    ok, why = gate(challenger_metric, champion_metric if current else None, higher_is_better, tolerance, floor)
    client.set_model_version_tag(name, version, "validation_status", "passed" if ok else "failed")
    client.set_model_version_tag(name, version, "validation_reason", why)
    if ok:
        if current:
            client.set_model_version_tag(name, version, "previous_champion", current)
        client.set_registered_model_alias(name, "champion", version)
    return GateResult(ok, why, version, current, challenger_metric, champion_metric)


def rollback(client: MlflowClient, name: str) -> tuple[str, str]:
    cur = client.get_model_version_by_alias(name, "champion")
    prev = cur.tags.get("previous_champion")
    if not prev:
        raise RuntimeError(f"{name} v{cur.version} has no previous_champion tag; nothing to roll back to")
    client.set_registered_model_alias(name, "champion", prev)
    client.set_model_version_tag(name, cur.version, "rolled_back", "true")
    return cur.version, prev
