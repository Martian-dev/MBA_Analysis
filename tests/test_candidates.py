from pipeline.candidates import HistoricalCandidates


def test_warm_profile_uses_exported_als_candidates_and_cold_profile_uses_popularity():
    candidates = HistoricalCandidates({'1': 20, '2': 10}, [
        {'user_id': 10, 'product_id': 3, 'score': .9, 'rank': 1},
        {'user_id': 10, 'product_id': 4, 'score': .4, 'rank': 2},
    ])
    assert list(candidates.ranked('10'))[:2] == [(3, 'als_personalized'), (4, 'als_personalized')]
    assert list(candidates.ranked())[:2] == [(1, 'popularity_fallback'), (2, 'popularity_fallback')]
