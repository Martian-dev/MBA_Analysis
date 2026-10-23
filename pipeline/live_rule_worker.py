"""Kafka completed orders -> validated, atomic live association snapshots."""
import argparse
import csv
import json
import os
import time

from kafka import KafkaConsumer
from redis import Redis

from .consumer import decode_event
from .kafka_worker import consume
from .live_rules import LiveRuleRepository
from .observability import ActivityLog
from .topics import EVENTS_TOPIC


def catalog_ids(path):
    products = {}
    with open(path, newline='', encoding='utf-8') as handle:
        for row in csv.DictReader(handle):
            for prefix, name in (('antecedent', 'antecedents'), ('consequent', 'consequents')):
                products[row[name]] = int(row[prefix + '_product_id'])
    return products


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rules', default=os.getenv('RULES_PATH', 'app/rules.csv'))
    parser.add_argument('--group-id', default='mba-live-rule-mining-v1')
    parser.add_argument('--max-events', type=int, default=0)
    args = parser.parse_args()
    client = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'), decode_responses=True)
    repository = LiveRuleRepository(client, catalog_ids(args.rules))
    consumer = KafkaConsumer(EVENTS_TOPIC,
        bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        group_id=args.group_id, enable_auto_commit=False, auto_offset_reset='earliest')
    last_refresh = [0.0]
    versions = {}

    def refresh(force=False):
        if not force and time.monotonic() - last_refresh[0] < 15:
            return
        for snapshot in repository.refresh():
            if versions.get(snapshot['source']) != snapshot['version']:
                summary = {key: snapshot[key] for key in ('source', 'version', 'status',
                    'independent_shoppers', 'discovery_transactions', 'validation_transactions')}
                summary['rule_count'] = len(snapshot['rules'])
                ActivityLog(client).record('live_rules_published', summary)
                print(json.dumps(summary), flush=True)
                versions[snapshot['source']] = snapshot['version']
        last_refresh[0] = time.monotonic()

    def handle(message):
        try:
            payload = decode_event(message.value)
        except ValueError as exc:
            ActivityLog(client).record('mining_event_rejected', {'reason': str(exc), 'offset': message.offset})
        else:
            repository.ingest(payload)

    try:
        refresh(force=True)
        consume(consumer, handle, idle=refresh, max_events=args.max_events)
        refresh(force=True)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
