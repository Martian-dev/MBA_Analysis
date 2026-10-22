from types import SimpleNamespace
from kafka import TopicPartition
from kafka.errors import CommitFailedError
from redis import RedisError
from pipeline.kafka_worker import consume


class Consumer:
    config = {'max_poll_interval_ms': 10}
    partition = TopicPartition('events', 0)
    message = SimpleNamespace(topic='events', partition=0, offset=12)
    def __init__(self, fail_commit=False):
        self.polls = 0; self.commits = 0; self.seeks = []; self.fail_commit = fail_commit
    def poll(self, **kwargs):
        self.polls += 1
        return {self.partition: [self.message]}
    def commit(self, offsets):
        self.commits += 1
        if self.fail_commit and self.commits == 1:
            raise CommitFailedError('group ownership lost')
    def assignment(self):
        return {self.partition}
    def seek(self, partition, offset):
        self.seeks.append((partition, offset))


def test_lost_group_ownership_returns_to_poll_and_replays_before_committing():
    consumer = Consumer(fail_commit=True); observed = []
    consume(consumer, lambda message: observed.append(message.offset), max_events=1, sleep=lambda _: None)
    assert consumer.polls == 2 and observed == [12, 12]
    assert consumer.seeks == [(consumer.partition, 12)]


def test_backend_outage_does_not_prevent_polling_past_the_retry_deadline():
    consumer = Consumer(); calls = []; tick = iter(range(20))
    def handler(message):
        calls.append(message.offset)
        if len(calls) == 1: raise RedisError('backend unavailable')
    consume(consumer, handler, max_events=1, clock=lambda: next(tick), sleep=lambda _: None)
    assert consumer.polls == 2 and consumer.commits == 1
