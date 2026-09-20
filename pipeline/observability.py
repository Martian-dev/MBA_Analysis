"""Bounded Redis activity log read by the live operations viewer."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

STREAM_KEY = "mba:observer:activity:v1"
COUNTS_KEY = "mba:observer:counts:v1"
MAX_EVENTS = 1000


class ActivityLog:
    def __init__(self, client: Any):
        self.client = client

    def record(self, kind: str, payload: dict[str, Any]) -> str:
        observed_at = datetime.now(timezone.utc).isoformat()
        entry = {"kind": kind, "observed_at": observed_at, "payload": payload}
        pipe = self.client.pipeline()
        pipe.xadd(STREAM_KEY, {"json": json.dumps(entry)}, maxlen=MAX_EVENTS, approximate=True)
        pipe.hincrby(COUNTS_KEY, kind, 1)
        results = pipe.execute()
        event_id = results[0]
        return event_id.decode() if isinstance(event_id, bytes) else event_id

    def snapshot(self, limit: int = 60) -> dict[str, Any]:
        rows = self.client.xrevrange(STREAM_KEY, count=limit)
        events = [self._decode(row) for row in reversed(rows)]
        counts = {key: int(value) for key, value in self.client.hgetall(COUNTS_KEY).items()}
        return {"events": events, "counts": counts, "last_id": events[-1]["id"] if events else "0-0"}

    @staticmethod
    def _decode(row: tuple[Any, dict[Any, Any]]) -> dict[str, Any]:
        event_id, fields = row
        if isinstance(event_id, bytes):
            event_id = event_id.decode()
        raw = fields.get("json", fields.get(b"json"))
        if isinstance(raw, bytes):
            raw = raw.decode()
        return {"id": event_id, **json.loads(raw)}

    def follow(self, last_id: str):
        while True:
            for _, rows in self.client.xread({STREAM_KEY: last_id}, count=20, block=15000):
                for row in rows:
                    decoded = self._decode(row)
                    last_id = decoded["id"]
                    yield decoded
