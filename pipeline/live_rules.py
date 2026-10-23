"""Windowed singleton-rule mining from independent, completed baskets.

Candidate discovery and temporal validation use disjoint shoppers. Fisher's
one-sided test and Benjamini-Yekutieli correction guard against chance findings
across dependent pair tests. Synthetic evidence is never mixed with storefront
transactions. This mines associations, not causal effects.
"""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from itertools import combinations
import hashlib
import json
import os
import time
from typing import Any

from scipy.stats import false_discovery_control, fisher_exact

from .contracts import validate_event
from .coupon_engine import configured_coupon_policy

NAMESPACE = 'mba:live-rules:v1'
SOURCES = ('storefront', 'synthetic')


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


@dataclass(frozen=True)
class MiningPolicy:
    window_seconds: int = 86400
    max_transactions: int = 1000
    min_transactions: int = 200
    min_pair_count: int = 10
    min_antecedent_count: int = 10
    min_support: float = .012
    min_confidence: float = .10
    min_lift: float = 1.5
    max_adjusted_p: float = .05

    def __post_init__(self):
        if not (self.window_seconds > 0 and 20 <= self.min_transactions <= self.max_transactions <= 5000
                and self.min_pair_count >= 2 and self.min_antecedent_count >= self.min_pair_count
                and 0 < self.min_support <= 1 and 0 < self.min_confidence <= 1
                and self.min_lift > 1 and 0 < self.max_adjusted_p <= .05):
            raise ValueError('Invalid live mining policy')

    @classmethod
    def configured(cls):
        coupon = configured_coupon_policy()
        return cls(window_seconds=int(os.getenv('LIVE_RULE_WINDOW_SECONDS', '86400')),
            max_transactions=int(os.getenv('LIVE_RULE_MAX_TRANSACTIONS', '1000')),
            min_transactions=int(os.getenv('LIVE_RULE_MIN_TRANSACTIONS', '200')),
            min_pair_count=int(os.getenv('LIVE_RULE_MIN_PAIR_COUNT', '10')),
            min_antecedent_count=int(os.getenv('LIVE_RULE_MIN_ANTECEDENT_COUNT', '10')),
            min_support=coupon['min_support'], min_confidence=coupon['min_confidence'],
            min_lift=coupon['min_lift'])


def _counts(baskets):
    singles, pairs = Counter(), Counter()
    for basket in baskets:
        products = sorted(set(basket['products']))
        singles.update(products)
        pairs.update(combinations(products, 2))
    return singles, pairs


def _metrics(a, b, singles, pairs, total):
    count = pairs[tuple(sorted((a, b)))]
    n_a, n_b = singles[a], singles[b]
    return {'support': count / total, 'confidence': count / n_a if n_a else 0,
            'lift': count * total / (n_a * n_b) if n_a and n_b else 0,
            'pair_count': count, 'antecedent_count': n_a, 'consequent_count': n_b,
            'transactions': total}


def _qualified(metrics, policy):
    return (metrics['pair_count'] >= policy.min_pair_count
        and metrics['antecedent_count'] >= policy.min_antecedent_count
        and metrics['support'] >= policy.min_support
        and metrics['confidence'] >= policy.min_confidence and metrics['lift'] >= policy.min_lift)


def mine_rules(transactions: list[dict[str, Any]], source: str, policy: MiningPolicy) -> dict[str, Any]:
    """Pure deterministic analytics boundary; inputs are accepted completed orders."""
    if source not in SOURCES:
        raise ValueError('Unknown evidence source')
    # One order per shopper per window; duplicates cannot inflate either split.
    unique, orders = set(), []
    seen_orders = set()
    for row in sorted(transactions, key=lambda item: (item['timestamp'], item['order_id'])):
        if row['user_id'] in unique or row['order_id'] in seen_orders:
            continue
        unique.add(row['user_id']); seen_orders.add(row['order_id']); orders.append(row)
    midpoint = len(orders) // 2
    discovery, validation = orders[:midpoint], orders[midpoint:]
    result = {'source': 'live_' + source, 'evidence': 'completed_baskets', 'rules': [],
        'independent_shoppers': len(orders), 'discovery_transactions': len(discovery),
        'validation_transactions': len(validation), 'policy': asdict(policy),
        'method': 'singleton_counts_temporal_holdout_fisher_by',
        'status': 'collecting', 'tested_candidates': 0,
        'version': 'live-' + digest([source, asdict(policy), orders])[:20]}
    if len(orders) < policy.min_transactions:
        return result
    ds, dp = _counts(discovery)
    vs, vp = _counts(validation)
    candidates, p_values = [], []
    for left, right in sorted(dp):
        for a, b in ((left, right), (right, left)):
            training = _metrics(a, b, ds, dp, len(discovery))
            if not _qualified(training, policy):
                continue
            metrics = _metrics(a, b, vs, vp, len(validation))
            count, n_a, n_b, total = metrics['pair_count'], vs[a], vs[b], len(validation)
            p_value = float(fisher_exact([[count, n_a - count], [n_b - count, total - n_a - n_b + count]],
                                        alternative='greater').pvalue)
            candidates.append((a, b, training, metrics, p_value))
            p_values.append(p_value)
    adjusted = false_discovery_control(p_values, method='by') if p_values else []
    result.update(status='validated', tested_candidates=len(candidates))
    for (a, b, training, metrics, p_value), adjusted_p in zip(candidates, adjusted):
        if not _qualified(metrics, policy) or adjusted_p > policy.max_adjusted_p:
            continue
        result['rules'].append({'antecedents': a, 'consequents': b,
            **{key: metrics[key] for key in ('support', 'confidence', 'lift')},
            'source': result['source'], 'run_id': result['version'],
            'discovery': training, 'validation': metrics,
            'p_value': p_value, 'adjusted_p_value': float(adjusted_p)})
    return result


class LiveRuleRepository:
    """Bounded, replay-safe transactions and atomic expiring rule snapshots."""
    _INGEST = """
if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
redis.call('SET', KEYS[3], '1', 'EX', ARGV[5])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
local old = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[4])
for _, id in ipairs(old) do redis.call('HDEL', KEYS[1], id); redis.call('ZREM', KEYS[2], id) end
local excess = redis.call('ZCARD', KEYS[2]) - tonumber(ARGV[6])
if excess > 0 then
  local extra = redis.call('ZRANGE', KEYS[2], 0, excess - 1)
  for _, id in ipairs(extra) do redis.call('HDEL', KEYS[1], id); redis.call('ZREM', KEYS[2], id) end
end
redis.call('EXPIRE', KEYS[1], ARGV[5]); redis.call('EXPIRE', KEYS[2], ARGV[5])
return 1
"""

    def __init__(self, client, products: dict[str, int], *, policy=None, namespace=NAMESPACE, clock=time.time):
        self.client, self.products, self.namespace, self.clock = client, products, namespace, clock
        self.policy = policy or MiningPolicy.configured()

    def _key(self, source, suffix):
        return f'{self.namespace}:{source}:{suffix}'

    def ingest(self, payload):
        event = validate_event(payload)
        if event['event_type'] != 'checkout_completed':
            return 'ignored'
        source = event.get('traffic_source')
        if source not in SOURCES:
            return 'unattributed_transaction'
        products = sorted(set(event.get('product_ids', [])))
        if not 1 <= len(products) <= 50 or any(product not in self.products for product in products):
            return 'invalid_basket'
        timestamp = datetime.fromisoformat(event['event_time'].replace('Z', '+00:00')).timestamp()
        now = self.clock()
        if timestamp <= now - self.policy.window_seconds or timestamp > now + 60:
            return 'outside_window'
        row = {'order_id': event['order_id'], 'user_id': event['user_id'],
               'timestamp': timestamp, 'products': products}
        order_key = digest(event['order_id'])
        added = self.client.eval(self._INGEST, 3, self._key(source, 'transactions'),
            self._key(source, 'timeline'), self._key(source, 'seen:' + order_key),
            order_key, json.dumps(row), timestamp, now - self.policy.window_seconds,
            max(604800, self.policy.window_seconds), self.policy.max_transactions)
        return 'accepted' if added else 'duplicate_order'

    def refresh(self):
        snapshots = []
        # Serialize selection/publication across worker restarts and rebalances.
        with self.client.lock(self.namespace + ':publish-lock', timeout=60, blocking_timeout=5):
            for source in SOURCES:
                ids = self.client.zrangebyscore(self._key(source, 'timeline'),
                    self.clock() - self.policy.window_seconds, '+inf')
                values = self.client.hmget(self._key(source, 'transactions'), ids) if ids else []
                rows = [json.loads(raw) for raw in values if raw]
                snapshot = mine_rules(rows, source, self.policy)
                snapshot['mined_at'] = datetime.fromtimestamp(self.clock(), timezone.utc).isoformat()
                for rule in snapshot['rules']:
                    rule.update(antecedent_product_id=self.products[rule['antecedents']],
                        consequent_product_id=self.products[rule['consequents']],
                        mined_at=snapshot['mined_at'])
                self.client.set(self._key(source, 'published'), json.dumps(snapshot), ex=120)
                snapshots.append(snapshot)
        return snapshots

    def snapshots(self):
        values = self.client.mget([self._key(source, 'published') for source in SOURCES])
        return [json.loads(value) for value in values if value]


def live_snapshots(client):
    """Read only published aggregate evidence, never raw shopper/order records."""
    values = client.mget([f'{NAMESPACE}:{source}:published' for source in SOURCES])
    return [json.loads(value) for value in values if value]


def serving_live_rules(client):
    allow_synthetic = os.getenv('LIVE_RULES_ALLOW_SYNTHETIC', '0') == '1'
    snapshots = live_snapshots(client)
    # Real evidence wins if both independent cohorts validate the same pair.
    return [rule for source in ('live_synthetic', 'live_storefront')
        for snapshot in snapshots if snapshot['source'] == source
        and (source != 'live_synthetic' or allow_synthetic)
        for rule in snapshot['rules']]
