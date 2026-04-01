from __future__ import annotations

import time
from dataclasses import dataclass, asdict
from typing import Dict, List


@dataclass
class TelemetryEvent:
    node_name: str
    bundle: str
    started_at: float
    ended_at: float
    duration_ms: int
    status: str
    error: str | None = None
    retries: int = 0
    cost_estimate: float = 0.0


class TelemetryStore:
    def __init__(self) -> None:
        self.events: List[TelemetryEvent] = []

    def start(self) -> float:
        return time.perf_counter()

    def record(
        self,
        node_name: str,
        bundle: str,
        started_at: float,
        status: str,
        error: str | None = None,
        retries: int = 0,
        cost_estimate: float = 0.0,
    ) -> None:
        ended = time.perf_counter()
        self.events.append(
            TelemetryEvent(
                node_name=node_name,
                bundle=bundle,
                started_at=started_at,
                ended_at=ended,
                duration_ms=int((ended - started_at) * 1000),
                status=status,
                error=error,
                retries=retries,
                cost_estimate=cost_estimate,
            )
        )

    def export(self) -> List[Dict]:
        return [asdict(e) for e in self.events]