"""Deterministically prove that rewarded feedback changes later ranking."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .adaptive import AdaptiveRankingService, CandidateSnapshot, RankingRequest


DEFAULT_FIXTURE = Path(__file__).with_name("fixtures") / "adaptive_replay.json"


def run_replay(fixture_path: str | Path = DEFAULT_FIXTURE) -> dict[str, Any]:
    fixture = json.loads(Path(fixture_path).read_text(encoding="utf-8"))
    service = AdaptiveRankingService(**fixture.get("policy", {}))
    snapshot = CandidateSnapshot.from_records(fixture["snapshot_version"], fixture["candidates"])
    request = RankingRequest(**fixture["request"])

    first = service.rank(request, snapshot)
    feedback = dict(fixture["feedback"])
    if feedback.get("impression_id") == "$first":
        feedback["impression_id"] = first.impression_id
    result = service.consume_feedback(feedback)
    second = service.rank(request, snapshot)
    first_order = [item.product_id for item in first.items]
    second_order = [item.product_id for item in second.items]
    if first_order == second_order:
        raise RuntimeError("adaptive replay did not change the later ranking")
    return {
        "fixture": str(fixture_path),
        "snapshot_version": snapshot.version,
        "policy_version": first.policy_version,
        "first_impression_id": first.impression_id,
        "first_ranking": first_order,
        "feedback_status": result.status,
        "reward": result.reward,
        "second_impression_id": second.impression_id,
        "second_ranking": second_order,
        "ranking_changed": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    args = parser.parse_args()
    print(json.dumps(run_replay(args.fixture), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
