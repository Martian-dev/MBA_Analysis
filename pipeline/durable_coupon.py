"""Atomic event/state/coupon commit with an at-least-once Kafka outbox.

Redis is the coupon authority. Kafka deliveries carry stable coupon IDs, so a
crash after send may repeat a notification but cannot create another offer.
Run Redis with AOF enabled; state and dedup retention are seven days.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
from typing import Any

from .contracts import validate_event
from .coupon_engine import CouponEngine, UserState
from .observability import ActivityLog, CouponLedger, RedisCouponClaimStore, COUPON_RETENTION_SECONDS
from .experiment import arm_for, EXPOSURES_KEY

STATE_PREFIX = "mba:coupon-state:v2:"
EVENT_PREFIX = "mba:coupon-event:v2:"
OUTBOX_KEY = "mba:coupon-outbox:v2"


def digest(*parts):
    return hashlib.sha256(json.dumps(parts, separators=(",", ":")).encode()).hexdigest()


class TransactionClaims:
    def __init__(self, client, transaction):
        self.client = client
        self.transaction = transaction

    def claim(self, claim):
        key = RedisCouponClaimStore._key(claim)
        if self.client.get(key) is not None:
            return False
        self.transaction.set(key, "issued", ex=COUPON_RETENTION_SECONDS)
        return True

    def release(self, claim):
        raise RuntimeError("Claims are committed with delivery; retry the event transaction")


class DurableCouponProcessor:
    def __init__(self, client, rules, **policy):
        self.client = client
        self.rules = rules
        self.policy = policy

    def process(self, payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        event = validate_event(payload)
        session = digest(event['user_id'], event['session_id'])
        result_key = EVENT_PREFIX + digest(event['user_id'], event['event_id'])
        state_key = STATE_PREFIX + session
        with self.client.lock(STATE_PREFIX + "lock:" + session, timeout=30, blocking_timeout=5):
            existing = self.client.get(result_key)
            if existing:
                result = json.loads(existing)
                return result['decision'], result['reason']
            raw_state = self.client.get(state_key)
            state = UserState(**json.loads(raw_state)) if raw_state else UserState()
            transaction = self.client.pipeline(transaction=True)
            engine = CouponEngine(self.rules, claims=TransactionClaims(self.client, transaction), **self.policy)
            decision, reason = engine.evaluate(event, state)
            transaction.set(state_key, json.dumps(asdict(state)), ex=COUPON_RETENTION_SECONDS)
            transaction.set(result_key, json.dumps(dict(decision=decision, reason=reason)), ex=COUPON_RETENTION_SECONDS)
            ActivityLog.enqueue(transaction, "event_received", {**event, "reason": reason})
            if reason in {'coupon_issued', 'experiment_control'}:
                transaction.xadd(EXPOSURES_KEY, {'json': json.dumps({
                    'user_id': event['user_id'], 'event_time': event['event_time'],
                    'arm': arm_for(event['user_id'], self.policy.get('holdout_fraction', 0)),
                    'product_id': decision['product_id'] if decision else event.get('product_id'),
                    'event_id': event['event_id'], 'trigger': event['event_type'],
                })})
            if decision:
                CouponLedger.enqueue_issue(transaction, decision)
                transaction.hset(OUTBOX_KEY, decision['coupon_id'], json.dumps(decision))
                ActivityLog.enqueue(transaction, "coupon_issued", decision)
            elif event['event_type'] in {'product_view_ended', 'basket_opened', 'recommendation_clicked'}:
                ActivityLog.enqueue(transaction, "coupon_suppressed", {**event, "reason": reason})
            transaction.execute()
            return decision, reason

    def drain(self, publish, max_items=100) -> int:
        delivered = 0
        for coupon_id, raw in self.client.hscan_iter(OUTBOX_KEY, count=100):
            with self.client.lock(OUTBOX_KEY + ':lock:' + digest(coupon_id), timeout=30, blocking_timeout=5):
                raw = self.client.hget(OUTBOX_KEY, coupon_id)
                if raw is None:
                    continue
                decision = json.loads(raw)
                publish(decision)
                self.client.hdel(OUTBOX_KEY, coupon_id)
                delivered += 1
                if delivered >= max_items:
                    break
        return delivered
