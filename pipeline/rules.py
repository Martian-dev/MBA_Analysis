"""Redis rule snapshot loading and lookup."""

from __future__ import annotations

import csv
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

import redis
import pandas as pd
from .artifacts import validate_rules

RULE_PREFIX = "mba:rules:v1:"
CATALOG_KEY = "mba:catalog:v1"


def rule_key(antecedent: str) -> str:
    return f"{RULE_PREFIX}{antecedent}"


def load_rules(
    rules_path: str | os.PathLike[str],
    redis_url: str = "redis://localhost:6379/0",
    min_lift: float = 1.5,
) -> dict[str, int]:
    """Load the strongest rule per directed pair into Redis."""

    strongest: dict[tuple[str, str], dict[str, float]] = {}
    validate_rules(pd.read_csv(rules_path))
    input_rows = 0
    with Path(rules_path).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            input_rows += 1
            antecedent = row["antecedents"].strip()
            consequent = row["consequents"].strip()
            metrics = {
                "support": float(row["support"]),
                "confidence": float(row["confidence"]),
                "lift": float(row["lift"]),
            }
            if antecedent == consequent or metrics["lift"] < min_lift:
                continue
            pair = (antecedent, consequent)
            if pair not in strongest or metrics["lift"] > strongest[pair]["lift"]:
                strongest[pair] = metrics

    grouped: dict[str, dict[str, str]] = defaultdict(dict)
    for (antecedent, consequent), metrics in strongest.items():
        grouped[antecedent][consequent] = json.dumps(metrics, separators=(",", ":"))

    client = redis.Redis.from_url(redis_url, decode_responses=True)
    pipe = client.pipeline()
    for antecedent in client.smembers(CATALOG_KEY):
        pipe.delete(rule_key(antecedent))
    for antecedent, consequents in grouped.items():
        pipe.delete(rule_key(antecedent))
        pipe.hset(rule_key(antecedent), mapping=consequents)
    pipe.delete(CATALOG_KEY)
    if grouped:
        pipe.sadd(CATALOG_KEY, *grouped.keys())
    pipe.execute()

    return {
        "input_rows": input_rows,
        "directed_pairs": len(strongest),
        "antecedents": len(grouped),
        "min_lift": min_lift,
    }


class RedisRuleLookup:
    """Lookup adapter used by both the reference engine and future stream job."""

    def __init__(self, client: redis.Redis, prefix: str = RULE_PREFIX):
        self.client = client
        self.prefix = prefix

    def lookup(self, antecedent: str, consequent: str) -> dict[str, Any] | None:
        if os.getenv('LIVE_RULE_MINING_ENABLED', '0') == '1':
            from .live_rules import serving_live_rules
            for rule in reversed(serving_live_rules(self.client)):
                if rule['antecedents'] == antecedent and rule['consequents'] == consequent:
                    return rule
        raw = self.client.hget(f"{self.prefix}{antecedent}", consequent)
        return json.loads(raw) if raw else None
