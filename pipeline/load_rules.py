"""CLI for publishing the current rule artifact into Redis."""

from __future__ import annotations

import argparse
import json
import os

import redis

from .rules import CATALOG_KEY, load_rules, rule_key


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rules", default="app/rules.csv")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--min-lift", type=float, default=float(os.getenv("MIN_LIFT", "1.5")))
    args = parser.parse_args()
    # The artifact can change between runs. Remove stale rule keys from the
    # previous snapshot before publishing the current one, while preserving
    # unrelated Redis data and the activity log.
    client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    old_antecedents = client.smembers(CATALOG_KEY)
    stale_keys = [rule_key(antecedent) for antecedent in old_antecedents]
    if stale_keys:
        client.delete(*stale_keys)
    print(json.dumps(load_rules(args.rules, args.redis_url, args.min_lift), indent=2))


if __name__ == "__main__":
    main()
