"""Transactional local demo orders with migration of existing JSONL records."""
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3


class OrderStore:
    def __init__(self, path):
        self.legacy = Path(path)
        self.path = self.legacy if self.legacy.suffix in {'.db', '.sqlite3'} else self.legacy.with_suffix('.sqlite3')

    @contextmanager
    def transaction(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path, timeout=30)
        try:
            connection.execute('PRAGMA journal_mode=WAL')
            connection.execute('PRAGMA synchronous=FULL')
            connection.execute('CREATE TABLE IF NOT EXISTS orders (order_id TEXT PRIMARY KEY, payload TEXT NOT NULL, fingerprint TEXT)')
            connection.execute('CREATE TABLE IF NOT EXISTS migrations (name TEXT PRIMARY KEY)')
            connection.execute('CREATE TABLE IF NOT EXISTS order_events (event_id TEXT PRIMARY KEY, payload TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0)')
            connection.execute('BEGIN IMMEDIATE')
            if self.legacy != self.path and self.legacy.is_file() and not connection.execute(
                "SELECT 1 FROM migrations WHERE name='jsonl'").fetchone():
                for line in self.legacy.read_text(encoding='utf-8').splitlines():
                    if line.strip():
                        order = json.loads(line)
                        connection.execute('INSERT OR IGNORE INTO orders VALUES (?, ?, NULL)',
                                           (order['order_id'], json.dumps(order)))
                connection.execute("INSERT INTO migrations VALUES ('jsonl')")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def records(self):
        with self.transaction() as connection:
            return [json.loads(row[0]) for row in connection.execute('SELECT payload FROM orders ORDER BY rowid')]

    @staticmethod
    def enqueue_completion(connection, order):
        event = {'event_id': 'checkout-completed:' + order['order_id'], 'event_type': 'checkout_completed',
            'event_time': order['created_at'], 'user_id': order['user_id'], 'session_id': order['session_id'],
            'order_id': order['order_id'], 'traffic_source': 'storefront',
            'product_ids': [item['product_name'] for item in order['items']],
            'catalog_product_ids': [item['product_id'] for item in order['items']]}
        connection.execute('INSERT OR IGNORE INTO order_events (event_id, payload) VALUES (?, ?)',
            (event['event_id'], json.dumps(event)))

    def drain_events(self, publish, limit=100):
        with self.transaction() as connection:
            rows = list(connection.execute('SELECT event_id, payload FROM order_events WHERE delivered=0 ORDER BY rowid LIMIT ?', (limit,)))
        delivered = 0
        for event_id, payload in rows:
            publish(json.loads(payload))
            with self.transaction() as connection:
                connection.execute('UPDATE order_events SET delivered=1 WHERE event_id=?', (event_id,))
            delivered += 1
        return delivered
