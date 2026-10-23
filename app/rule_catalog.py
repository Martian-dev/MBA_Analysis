"""Read-only presentation of directed historical rules and coupon eligibility."""
from __future__ import annotations

from typing import Any, Callable, Iterable


def describe_rules(
    records: Iterable[dict[str, Any]],
    resolve_product: Callable,
    policy: dict[str, float | int],
) -> list[dict[str, Any]]:
    rows = []
    for record in records:
        left = resolve_product(record.get('antecedent_product_id') or record['antecedents'])
        right = resolve_product(record.get('consequent_product_id') or record['consequents'])
        if not left or not right:
            continue
        metrics = {key: float(record[key]) for key in ('support', 'confidence', 'lift')}
        failed = [key for key, value in metrics.items() if value < policy['min_' + key]]
        rows.append({
            'antecedent': {'product_id': left['product_id'], 'product_name': left['product_name']},
            'consequent': {'product_id': right['product_id'], 'product_name': right['product_name']},
            **metrics, 'coupon_eligible': not failed, 'failed_thresholds': failed,
            'source': record.get('source', 'historical_snapshot'),
            'version': record.get('run_id'), 'mined_at': record.get('mined_at'),
            'discovery': record.get('discovery'), 'validation': record.get('validation'),
            'adjusted_p_value': record.get('adjusted_p_value'),
        })
    return rows
