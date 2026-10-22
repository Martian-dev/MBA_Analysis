"""CLI for publishing the current rule artifact into Redis."""

from __future__ import annotations

import argparse
import json
import os

from .rules import load_rules


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rules", default=os.getenv("RULES_PATH", "app/rules.csv"))
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--min-lift", type=float, default=float(os.getenv("MIN_LIFT", "1.5")))
    args = parser.parse_args()
    # Validation and removal of stale keys share the loader's atomic publish.
    print(json.dumps(load_rules(args.rules, args.redis_url, args.min_lift), indent=2))


if __name__ == "__main__":
    main()
