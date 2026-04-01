from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict

from nodes.node_contracts import ALLOWED_WRITE_KEYS, PROTECTED_KEYS, NodeResult


def _safe_equal(a: Any, b: Any) -> bool:
    try:
        return a == b
    except Exception:
        return False


def compute_patch(before: Dict[str, Any], after: Dict[str, Any], node_name: str) -> Dict[str, Any]:
    allowed = ALLOWED_WRITE_KEYS.get(node_name)
    patch: Dict[str, Any] = {}

    for key, value in after.items():
        if key not in before or not _safe_equal(before.get(key), value):
            if allowed is None or key in allowed:
                patch[key] = deepcopy(value)

    return patch


def merge_patch(
    state: Dict[str, Any],
    result: NodeResult,
    allow_protected: bool = False,
) -> Dict[str, Any]:
    merged = deepcopy(state)

    if result.status not in {"success", "degraded"}:
        return merged

    for key, value in result.patch.items():
        if key in PROTECTED_KEYS and not allow_protected:
            continue
        merged[key] = value

    return merged