import pandas as pd
import pytest
from pipeline.artifacts import singleton_rules, validate_rules


def test_multi_item_context_is_never_flattened_into_a_singleton_rule():
    frame = pd.DataFrame([
        dict(antecedents=frozenset({'A', 'B'}), consequents=frozenset({'C'}), support=.1, confidence=.9, lift=4),
        dict(antecedents=frozenset({'A'}), consequents=frozenset({'C'}), support=.2, confidence=.3, lift=1.5),
    ])
    exported = singleton_rules(frame)
    assert len(exported) == 1
    assert exported.iloc[0]['confidence'] == .3


def test_conflicting_duplicate_metrics_fail_snapshot_validation():
    frame = pd.DataFrame([
        dict(antecedents='A', consequents='C', support=.1, confidence=.9, lift=4),
        dict(antecedents='A', consequents='C', support=.2, confidence=.3, lift=1.5),
    ])
    with pytest.raises(ValueError, match='duplicate'):
        validate_rules(frame)
