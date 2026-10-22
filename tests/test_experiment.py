from datetime import datetime, timezone
from pipeline.experiment import arm_for, summarize


def test_user_assignment_is_stable_and_has_explicit_boundary_values():
    assert arm_for('u', 0) == 'treatment'
    assert arm_for('u', 1) == 'control'
    assert arm_for('u', .5) == arm_for('u', .5)


def test_incomplete_observation_window_is_not_reported_as_non_conversion():
    report = summarize([{'user_id': 'u', 'arm': 'control', 'event_time': '2026-10-22T00:00:00Z'}], [],
                       as_of=datetime(2026, 10, 23, tzinfo=timezone.utc))
    assert report['status'] == 'awaiting_mature_cohorts'
    assert report['groups']['control']['conversion_rate'] is None


def test_orders_before_eligibility_do_not_count_as_coupon_conversions():
    exposures = [{'user_id': 'u', 'arm': 'treatment', 'event_time': '2026-10-01T00:00:00Z'}]
    orders = [{'user_id': 'u', 'created_at': '2026-09-30T00:00:00Z', 'total_inr': 100, 'savings_inr': 5}]
    report = summarize(exposures, orders, as_of=datetime(2026, 10, 22, tzinfo=timezone.utc))
    assert report['groups']['treatment']['conversion_rate'] == 0
    assert report['groups']['treatment']['illustrative_revenue_inr'] == 0
