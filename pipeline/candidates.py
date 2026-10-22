"""Historical candidate sources, with explicit cold-start behavior."""
from collections import Counter, defaultdict
import json
from pathlib import Path
from itertools import zip_longest


class HistoricalCandidates:
    def __init__(self, popularity=None, als_records=()):
        self.popularity = {int(key): float(value) for key, value in (popularity or {}).items()}
        self.by_user = defaultdict(list)
        self.collaborative = Counter()
        for record in als_records:
            product_id = int(record['product_id'])
            score = float(record['score'])
            self.by_user[str(record['user_id'])].append((product_id, score))
            self.collaborative[product_id] += 1 / max(1, int(record.get('rank', 1)))

    @classmethod
    def load(cls, popularity_path, als_path=None):
        popularity_path = Path(popularity_path)
        popularity = json.loads(popularity_path.read_text()) if popularity_path.is_file() else {}
        records = []
        if als_path:
            path = Path(als_path)
            files = sorted(path.glob('part-*.json')) if path.is_dir() else [path] if path.is_file() else []
            for file in files:
                records.extend(json.loads(line) for line in file.read_text().splitlines() if line.strip())
        return cls(popularity, records)

    @property
    def has_collaborative(self):
        return bool(self.collaborative)

    def ranked(self, historical_user_id=None):
        if historical_user_id is not None and str(historical_user_id) in self.by_user:
            for product_id, score in sorted(self.by_user[str(historical_user_id)], key=lambda item: (-item[1], item[0])):
                yield product_id, 'als_personalized'
        # Explicit 1:1 candidate blending prevents a large popularity catalog
        # from starving the collaborative source. Anonymous users use aggregate
        # collaborative evidence, never an arbitrary historical user's profile.
        popular = sorted(self.popularity, key=lambda product: (-self.popularity[product], product))
        collaborative = sorted(self.collaborative, key=lambda product: (-self.collaborative[product], product))
        for als_product, popular_product in zip_longest(collaborative, popular):
            if als_product is not None:
                yield als_product, 'als_cold_start_fallback'
            if popular_product is not None:
                yield popular_product, 'popularity_fallback'
