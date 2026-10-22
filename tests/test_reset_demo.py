from pipeline.coupon_engine import CouponClaim
from pipeline.observability import (
    COUPON_CLAIM_KEY_PREFIX,
    COUPON_KEY_PREFIX,
    CouponLedger,
    RedisCouponClaimStore,
)
from pipeline.reset_demo import reset_redis


class MemoryPipeline:
    def __init__(self, client):
        self.client = client
        self.operations = []

    def hgetall(self, key):
        self.operations.append(("hgetall", key))
        return self

    def hset(self, key, mapping):
        self.operations.append(("hset", key, mapping))
        return self

    def expire(self, key, seconds):
        self.operations.append(("expire", key, seconds))
        return self

    def execute(self):
        results = []
        for operation in self.operations:
            if operation[0] == "hgetall":
                results.append(dict(self.client.hashes.get(operation[1], {})))
            elif operation[0] == "hset":
                self.client.hset(operation[1], operation[2])
                results.append(1)
            else:
                results.append(True)
        return results


class MemoryRedis:
    def __init__(self):
        self.hashes = {}
        self.values = {"mba:observer:activity:v1": "activity", "mba:observer:counts:v1": "counts"}

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)

    def set(self, key, value, *, nx=False, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    def eval(self, script, key_count, *values):
        del script
        keys = values[:key_count]
        args = values[key_count:]
        redeemed = []
        for index, key in enumerate(keys):
            record = self.hashes.get(key)
            if (
                record
                and record.get("user_id") == args[0]
                and record.get("session_id") == args[1]
                and record.get("redeemed_order_id") in {None, args[2]}
            ):
                record.update(redeemed_order_id=args[2], redeemed_at=args[3])
                redeemed.append(args[index + 4])
        return redeemed

    def pipeline(self):
        return MemoryPipeline(self)

    def scan_iter(self, match, count):
        del count
        prefix = match.removesuffix("*")
        keys = set(self.hashes) | set(self.values)
        return (key for key in keys if key.startswith(prefix))

    def delete(self, *keys):
        for key in keys:
            self.hashes.pop(key, None)
            self.values.pop(key, None)


def test_demo_reset_clears_redemption_ledger_for_a_fresh_coupon(monkeypatch):
    client = MemoryRedis()
    ledger = CouponLedger(client)
    coupon = {
        "coupon_id": "coupon:user:session:Banana",
        "user_id": "user",
        "session_id": "session",
        "product_id": "Banana",
        "discount_percent": 5,
        "event_time": "2026-10-21T12:00:00+00:00",
    }
    ledger.issue(coupon)
    claim_store = RedisCouponClaimStore(client)
    coupon_claim = CouponClaim("user", "session", "Banana")
    claim_store.claim(coupon_claim)
    assert ledger.redeem(
        [coupon["coupon_id"]], user_id="user", session_id="session", order_id="BS-first"
    )
    assert ledger.redeem(
        [coupon["coupon_id"]], user_id="user", session_id="session", order_id="BS-first"
    )
    assert not ledger.redeem(
        [coupon["coupon_id"]], user_id="user", session_id="session", order_id="BS-replay"
    )

    monkeypatch.setattr("pipeline.reset_demo.redis.Redis.from_url", lambda *args, **kwargs: client)
    reset_redis("redis://example")
    assert not any(key.startswith(COUPON_KEY_PREFIX) for key in client.hashes)
    assert claim_store._key(coupon_claim) not in client.values

    ledger.issue(coupon)
    assert ledger.redeem(
        [coupon["coupon_id"]], user_id="user", session_id="session", order_id="BS-fresh"
    )


def test_available_coupons_are_unredeemed_and_can_be_scoped_to_one_session():
    client = MemoryRedis()
    ledger = CouponLedger(client)
    first = {
        "coupon_id": "coupon:first",
        "user_id": "user",
        "session_id": "session-a",
        "product_id": "Banana",
        "discount_percent": 5,
        "event_time": "2026-10-21T12:00:00+00:00",
    }
    second = {
        **first,
        "coupon_id": "coupon:second",
        "session_id": "session-b",
        "product_id": "Organic Avocado",
        "event_time": "2026-10-21T12:01:00+00:00",
    }
    ledger.issue(first)
    ledger.issue(second)

    assert [coupon["coupon_id"] for coupon in ledger.available()] == [
        "coupon:second", "coupon:first"
    ]
    assert [coupon["coupon_id"] for coupon in ledger.available(
        user_id="user", session_id="session-a"
    )] == ["coupon:first"]

    ledger.redeem(
        [first["coupon_id"]],
        user_id="user",
        session_id="session-a",
        order_id="BS-redeemed",
    )
    assert [coupon["coupon_id"] for coupon in ledger.available()] == ["coupon:second"]


def test_redis_coupon_claim_is_atomic_and_session_scoped():
    claims = RedisCouponClaimStore(MemoryRedis())
    first_session = CouponClaim("user", "session-a", "Banana")
    second_session = CouponClaim("user", "session-b", "Banana")

    assert claims.claim(first_session) is True
    assert claims.claim(first_session) is False
    claims.release(first_session)
    assert claims.claim(first_session) is True
    assert claims.claim(second_session) is True
    assert claims._key(first_session).startswith(COUPON_CLAIM_KEY_PREFIX)
