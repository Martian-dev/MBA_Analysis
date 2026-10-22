"""User-level coupon holdout assignment and observation-window reporting.

The local checkout uses illustrative prices. These measurements are demo
outcomes; causal profit/ROI requires real purchases and real contribution costs.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path


EXPOSURES_KEY = 'mba:experiment:eligible:v1'


def arm_for(user_id, holdout_fraction=0.0):
    if not 0 <= holdout_fraction <= 1:
        raise ValueError('holdout fraction must be between zero and one')
    bucket = int(hashlib.sha256(('coupon-holdout-v1:' + user_id).encode()).hexdigest(), 16) / 2**256
    return 'control' if bucket < holdout_fraction else 'treatment'


def summarize(exposures, orders, *, as_of, observation_days=7):
    users = {}
    for exposure in sorted(exposures, key=lambda row: row['event_time']):
        users.setdefault(exposure['user_id'], exposure)
    groups = {}
    for arm in ('control', 'treatment'):
        mature = {user: row for user, row in users.items() if row['arm'] == arm and
                  datetime.fromisoformat(row['event_time'].replace('Z', '+00:00')) + timedelta(days=observation_days) <= as_of}
        conversions = set(); revenue = 0; discounts = 0
        for order in orders:
            exposure = mature.get(order['user_id'])
            if exposure:
                start = datetime.fromisoformat(exposure['event_time'].replace('Z', '+00:00'))
                placed = datetime.fromisoformat(order['created_at'].replace('Z', '+00:00'))
                if start <= placed <= start + timedelta(days=observation_days):
                    conversions.add(order['user_id']); revenue += order['total_inr']; discounts += order['savings_inr']
        groups[arm] = dict(mature_users=len(mature), converted_users=len(conversions),
                          conversion_rate=len(conversions) / len(mature) if mature else None,
                          illustrative_revenue_inr=revenue, illustrative_discount_inr=discounts)
    return dict(status='observed' if all(group['mature_users'] for group in groups.values()) else 'awaiting_mature_cohorts',
                groups=groups, observation_days=observation_days,
                real_world_roi='not_measured_demo_prices_and_no_real_purchase_experiment')


def main():
    from redis import Redis
    from app.order_store import OrderStore
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--redis-url', default='redis://localhost:6379/0')
    parser.add_argument('--orders', type=Path, default=Path('.runtime/marketplace/orders.jsonl'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = Redis.from_url(args.redis_url, decode_responses=True).xrange(EXPOSURES_KEY)
    report = summarize([json.loads(fields['json']) for _, fields in rows], OrderStore(args.orders).records(),
                       as_of=datetime.now(timezone.utc))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
