import pytest
from pipeline.evaluation import evaluate, mine_pairs, score_ranking


def test_pair_metrics_use_exact_baskets_and_preserve_both_directions():
    rules, _ = mine_pairs([{1, 2, 3}, {1, 2}, {1}, {4}], min_count=1, min_support=0, min_confidence=0, min_lift=0)
    pair = next(rule for rule in rules if rule['antecedent_product_id'] == 1 and rule['consequent_product_id'] == 2)
    assert pair['support'] == .5
    assert pair['confidence'] == pytest.approx(2 / 3)
    assert pair['lift'] == pytest.approx(4 / 3)
    assert len({(row['antecedent_product_id'], row['consequent_product_id']) for row in rules}) == len(rules)


def test_hidden_products_do_not_leak_into_training_candidates():
    report, rules, singles = evaluate([{1, 2}, {1, 2}], [(9, {3, 4})], k=2, min_count=1)
    assert set(singles) == {1, 2}
    assert report['metrics']['popularity']['recall'] == 0
    assert report['metrics']['association_rules']['recall'] == 0
    assert report['coupon_roi'].startswith('not_measured')


def test_ranking_metrics_have_independently_known_values():
    metrics = score_ranking([9, 2], {2, 3}, 2)
    assert metrics['precision'] == .5
    assert metrics['recall'] == .5
    assert metrics['hit_rate'] == 1
    assert metrics['ndcg'] == pytest.approx(.3868528072)
