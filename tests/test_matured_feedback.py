from pipeline.adaptive import create_adaptive_service


def test_unanswered_display_matures_once_and_does_not_close_early():
    now = [1000]
    service = create_adaptive_service(clock=lambda: now[0], feedback_window_seconds=30)
    decision = service.rank_candidates(user_id='u', session_id='s', snapshot_version='v',
        candidates=[{'product_id': 'A', 'prior_score': 1}])
    event = dict(event_id='expose', event_type='recommendation_impression', product_id='A',
                 user_id='u', session_id='s', event_time='2026-10-22T12:00:00Z',
                 impression_id=decision['impression_id'])
    service.observe_event(event)
    assert service.settle_expired() == 0
    now[0] += 31
    assert service.settle_expired() == 1
    assert service.settle_expired() == 0


def test_clicked_display_does_not_receive_a_second_zero_reward():
    now = [1000]
    service = create_adaptive_service(clock=lambda: now[0], feedback_window_seconds=30)
    decision = service.rank_candidates(user_id='u', session_id='s', snapshot_version='v',
        candidates=[{'product_id': 'A', 'prior_score': 1}])
    event = dict(event_id='expose', event_type='recommendation_impression', product_id='A',
                 user_id='u', session_id='s', event_time='2026-10-22T12:00:00Z',
                 impression_id=decision['impression_id'])
    service.observe_event(event)
    assert service.observe_event({**event, 'event_id': 'click', 'event_type': 'recommendation_clicked'}).status == 'learned'
    now[0] += 31
    assert service.settle_expired() == 0
