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
