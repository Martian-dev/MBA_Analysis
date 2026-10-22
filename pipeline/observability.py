"""Bounded Redis activity log read by the live operations viewer."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from .coupon_engine import CouponClaim

STREAM_KEY = "mba:observer:activity:v1"
COUNTS_KEY = "mba:observer:counts:v1"
MAX_EVENTS = 1000
COUPON_KEY_PREFIX = "mba:coupon:v1:"
COUPON_CLAIM_KEY_PREFIX = "mba:coupon-claim:v1:"
COUPON_RETENTION_SECONDS = 7 * 24 * 60 * 60


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

    def snapshot(
        self,
        limit: int = 60,
        user_id: str | None = None,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        scoped = user_id is not None or session_id is not None
        rows = self.client.xrevrange(STREAM_KEY, count=MAX_EVENTS if scoped else limit)
        events = [self._decode(row) for row in reversed(rows)]
        if scoped:
            events = [
                event for event in events
                if event["payload"].get("user_id") == user_id
                and event["payload"].get("session_id") == session_id
            ][-limit:]
            counts = dict(Counter(event["kind"] for event in events))
        else:
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


class CouponLedger:
    """Durable coupon authority with atomic, one-time redemption."""

    _REDEEM_SCRIPT = """
local redeemed = {}
for index, key in ipairs(KEYS) do
    if redis.call('EXISTS', key) == 1
        and redis.call('HGET', key, 'user_id') == ARGV[1]
        and redis.call('HGET', key, 'session_id') == ARGV[2]
        and (not redis.call('HGET', key, 'redeemed_order_id')
            or redis.call('HGET', key, 'redeemed_order_id') == ARGV[3]) then
        redis.call('HSET', key, 'redeemed_order_id', ARGV[3], 'redeemed_at', ARGV[4])
        table.insert(redeemed, ARGV[index + 4])
    end
end
return redeemed
"""

    def __init__(self, client: Any):
        self.client = client

    @staticmethod
    def _key(coupon_id: str) -> str:
        return f"{COUPON_KEY_PREFIX}{coupon_id}"

    def issue(self, coupon: dict[str, Any]) -> None:
        fields = {
            "coupon_id": str(coupon["coupon_id"]),
            "user_id": str(coupon["user_id"]),
            "session_id": str(coupon["session_id"]),
            "product_id": str(coupon["product_id"]),
            "discount_percent": str(coupon["discount_percent"]),
            "issued_at": str(coupon["event_time"]),
        }
        key = self._key(fields["coupon_id"])
        pipe = self.client.pipeline()
        pipe.hset(key, mapping=fields)
        pipe.expire(key, COUPON_RETENTION_SECONDS)
        pipe.execute()

    def available(
        self,
        *,
        user_id: str | None = None,
        session_id: str | None = None,
    ) -> list[dict[str, str]]:
        """Return active, unredeemed coupons, optionally for one session."""
        keys = list(self.client.scan_iter(match=f"{COUPON_KEY_PREFIX}*", count=200))
        if not keys:
            return []
        pipe = self.client.pipeline()
        for key in keys:
            pipe.hgetall(key)
        coupons = []
        for record in pipe.execute():
            normalized = {
                (key.decode() if isinstance(key, bytes) else str(key)):
                (value.decode() if isinstance(value, bytes) else str(value))
                for key, value in record.items()
            }
            if not normalized or normalized.get("redeemed_order_id"):
                continue
            if user_id is not None and normalized.get("user_id") != user_id:
                continue
            if session_id is not None and normalized.get("session_id") != session_id:
                continue
            coupons.append(normalized)
        coupons.sort(key=lambda coupon: coupon.get("issued_at", ""), reverse=True)
        return coupons

    def redeem(
        self,
        coupon_ids: list[str],
        *,
        user_id: str,
        session_id: str,
        order_id: str,
        allowed_product_ids: set[str] | None = None,
    ) -> list[dict[str, str]]:
        unique_ids = list(dict.fromkeys(coupon_ids))
        if not unique_ids:
            return []
        if allowed_product_ids is not None:
            pipe = self.client.pipeline()
            for coupon_id in unique_ids:
                pipe.hgetall(self._key(coupon_id))
            records = pipe.execute()
            unique_ids = [
                coupon_id
                for coupon_id, record in zip(unique_ids, records)
                if str(record.get("product_id", "")) in allowed_product_ids
            ]
            if not unique_ids:
                return []
        redeemed_at = datetime.now(timezone.utc).isoformat()
        redeemed_ids = self.client.eval(
            self._REDEEM_SCRIPT,
            len(unique_ids),
            *(self._key(coupon_id) for coupon_id in unique_ids),
            user_id,
            session_id,
            order_id,
            redeemed_at,
            *unique_ids,
        )
        normalized_ids = [
            value.decode() if isinstance(value, bytes) else str(value)
            for value in redeemed_ids
        ]
        pipe = self.client.pipeline()
        for coupon_id in normalized_ids:
            pipe.hgetall(self._key(coupon_id))
        return [dict(record) for record in pipe.execute()]


class RedisCouponClaimStore:
    """Atomic session/product frequency cap shared by coupon consumers."""

    def __init__(self, client: Any, ttl_seconds: int = COUPON_RETENTION_SECONDS):
        self.client = client
        self.ttl_seconds = ttl_seconds

    @staticmethod
    def _key(coupon_claim: CouponClaim) -> str:
        identity = "\0".join((
            coupon_claim.user_id,
            coupon_claim.session_id,
            coupon_claim.product_id,
        )).encode("utf-8")
        return f"{COUPON_CLAIM_KEY_PREFIX}{hashlib.sha256(identity).hexdigest()}"

    def claim(self, coupon_claim: CouponClaim) -> bool:
        return bool(self.client.set(
            self._key(coupon_claim),
            "issued",
            nx=True,
            ex=self.ttl_seconds,
        ))

    def release(self, coupon_claim: CouponClaim) -> None:
        self.client.delete(self._key(coupon_claim))
