"""Chronological basket-completion evaluation and exact singleton rule mining.

Only earlier orders train the model. Each user's final known basket is split
into context and hidden targets using a fixed seed. No historical user IDs are
assigned to new browser shoppers. This measures recommendations, not coupon ROI.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
from itertools import combinations
import json
import math
from pathlib import Path
import random

import pandas as pd


def mine_pairs(baskets, *, min_count=5, min_support=0.001, min_confidence=0.05, min_lift=1.0):
    baskets = [set(basket) for basket in baskets if basket]
    singles = Counter(item for basket in baskets for item in basket)
    active = {item for item, count in singles.items() if count >= min_count}
    pairs = Counter(pair for basket in baskets for pair in combinations(sorted(basket & active), 2))
    total = len(baskets)
    rules = []
    for (left, right), count in sorted(pairs.items()):
        support = count / total
        lift = count * total / (singles[left] * singles[right])
        if count < min_count or support < min_support or lift < min_lift:
            continue
        for antecedent, consequent in ((left, right), (right, left)):
            confidence = count / singles[antecedent]
            if confidence >= min_confidence:
                rules.append(dict(antecedent_product_id=antecedent, consequent_product_id=consequent,
                                  support=support, confidence=confidence, lift=lift))
    return rules, singles


def score_ranking(ranked, targets, k):
    hits = [int(item in targets) for item in ranked[:k]]
    found = sum(hits)
    dcg = sum(hit / math.log2(index + 2) for index, hit in enumerate(hits))
    ideal = sum(1 / math.log2(index + 2) for index in range(min(k, len(targets))))
    return {"hit_rate": float(found > 0), "precision": found / k,
            "recall": found / len(targets), "ndcg": dcg / ideal if ideal else 0.0}


def evaluate(train_baskets, heldout, *, k=10, seed=42, **mining):
    rules, singles = mine_pairs(train_baskets, **mining)
    by_item = defaultdict(list)
    for rule in rules:
        by_item[rule["antecedent_product_id"]].append(rule)
    popular = sorted(singles, key=lambda item: (-singles[item], item))
    rows = {name: [] for name in ("popularity", "association_rules", "rules_with_popularity_fallback")}
    recommended = {name: set() for name in rows}
    for order_id, basket in sorted(heldout):
        items = sorted(set(basket))
        if len(items) < 2:
            continue
        random.Random(f"{seed}:{order_id}").shuffle(items)
        context = set(items[:max(1, len(items) // 2)])
        targets = set(items) - context
        scores = {}
        for item in context:
            for rule in by_item[item]:
                product = rule["consequent_product_id"]
                if product not in context:
                    scores[product] = max(scores.get(product, 0), rule["confidence"] * math.log1p(rule["lift"]))
        associated = sorted(scores, key=lambda item: (-scores[item], item))
        baseline = [item for item in popular if item not in context]
        rankings = {"popularity": baseline[:k], "association_rules": associated[:k],
                    "rules_with_popularity_fallback": list(dict.fromkeys(associated + baseline))[:k]}
        for name, ranked in rankings.items():
            rows[name].append(score_ranking(ranked, targets, k))
            recommended[name].update(ranked)
    if not rows["popularity"]:
        raise ValueError("No held-out baskets with at least two products")
    metrics = {}
    for name, observations in rows.items():
        metrics[name] = {metric: sum(row[metric] for row in observations) / len(observations)
                         for metric in observations[0]}
        metrics[name]["catalog_coverage"] = len(recommended[name]) / max(1, len(singles))
    # Paired bootstrap retains the shared held-out basket for each comparison.
    differences = [a["ndcg"] - b["ndcg"] for a, b in zip(
        rows["rules_with_popularity_fallback"], rows["popularity"])]
    rng = random.Random(seed)
    draws = sorted(sum(rng.choices(differences, k=len(differences))) / len(differences) for _ in range(500))
    return {"protocol": "last-order holdout; deterministic half-basket completion",
            "seed": seed, "k": k, "training_baskets": len(train_baskets),
            "evaluated_baskets": len(differences), "rule_count": len(rules), "metrics": metrics,
            "hybrid_minus_popularity_ndcg_95ci": [draws[12], draws[487]],
            "coupon_roi": "not_measured_requires_randomized_real_shopper_outcomes"}, rules, singles


def load_split(data_dir, users=2000, seed=42):
    orders = pd.read_csv(data_dir / "orders.csv", usecols=["order_id", "user_id", "order_number", "eval_set"])
    orders = orders[orders.eval_set.isin(["prior", "train"])]
    counts = orders.groupby("user_id").size()
    eligible = sorted(counts[counts >= 3].index.tolist())
    chosen = set(random.Random(seed).sample(eligible, min(users, len(eligible))))
    selected = orders[orders.user_id.isin(chosen)].sort_values(["user_id", "order_number", "order_id"])
    test_orders = set(selected.groupby("user_id").tail(1).order_id)
    order_ids = set(selected.order_id)
    baskets = defaultdict(set)
    for filename in ("order_products__prior.csv", "order_products__train.csv"):
        for chunk in pd.read_csv(data_dir / filename, usecols=["order_id", "product_id"], chunksize=500_000):
            chunk = chunk[chunk.order_id.isin(order_ids)]
            for order_id, product_id in chunk.itertuples(index=False, name=None):
                baskets[int(order_id)].add(int(product_id))
    train = [basket for order_id, basket in sorted(baskets.items()) if order_id not in test_orders]
    heldout = [(order_id, basket) for order_id, basket in sorted(baskets.items()) if order_id in test_orders]
    if not train:
        raise ValueError("No training baskets; choose users with at least three known orders")
    split = {"users": len(chosen), "training_order_ids": sorted(set(baskets) - test_orders),
             "heldout_order_ids": sorted(test_orders)}
    split["sha256"] = hashlib.sha256(json.dumps(split, sort_keys=True).encode()).hexdigest()
    return train, heldout, split


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=Path("InstacartMBA"))
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--users", type=int, default=2000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--k", type=int, default=10)
    args = parser.parse_args()
    if args.users < 1 or args.k < 1:
        parser.error("users and k must be positive")
    train, heldout, split = load_split(args.data_dir, args.users, args.seed)
    report, rules, singles = evaluate(train, heldout, k=args.k, seed=args.seed)
    names = dict(pd.read_csv(args.data_dir / "products.csv", usecols=["product_id", "product_name"]).itertuples(index=False, name=None))
    args.output_dir.mkdir(parents=True, exist_ok=True)
    run_id = f"evaluated-{split['sha256'][:16]}"
    fields = ["antecedent_product_id", "consequent_product_id", "antecedents", "consequents", "support", "confidence", "lift", "run_id"]
    with (args.output_dir / "rules_serving.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator='\n'); writer.writeheader()
        for rule in rules:
            writer.writerow({**rule, "antecedents": names[rule["antecedent_product_id"]],
                             "consequents": names[rule["consequent_product_id"]], "run_id": run_id})
    report.update(run_id=run_id, split_sha256=split["sha256"], mining={"min_count": 5, "min_support": .001, "min_confidence": .05, "min_lift": 1.0})
    for filename, value in (("evaluation.json", report), ("split.json", split),
                            ("popularity.json", {str(key): value for key, value in singles.items()})):
        (args.output_dir / filename).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
