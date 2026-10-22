"""Validate and promote exact product-level rule snapshots."""
import argparse
import math
from pathlib import Path
import shutil

import pandas as pd


def validate_rules(frame):
    required = {'antecedents', 'consequents', 'support', 'confidence', 'lift'}
    if not required <= set(frame.columns):
        raise ValueError('Rule snapshot is missing required columns')
    if frame.empty:
        raise ValueError('Rule snapshot is empty')
    for column in ('antecedents', 'consequents'):
        if frame[column].isna().any() or not frame[column].map(lambda value: isinstance(value, str) and bool(value.strip())).all():
            raise ValueError('Rule product names must be non-empty strings')
    for column in ('support', 'confidence', 'lift'):
        values = pd.to_numeric(frame[column], errors='coerce')
        if not values.map(math.isfinite).all() or (values <= 0).any():
            raise ValueError(f'{column} must contain finite positive metrics')
        if column != 'lift' and (values > 1).any():
            raise ValueError(f'{column} cannot exceed one')
    if (frame.antecedents == frame.consequents).any():
        raise ValueError('Self-referencing rule')
    if frame.duplicated(['antecedents', 'consequents']).any():
        raise ValueError('Ambiguous duplicate pairs; re-mine genuine singleton rules')
    return frame


def singleton_rules(frame):
    """Filter itemsets before extracting their item; never discard context."""
    result = frame[frame.antecedents.map(len).eq(1) & frame.consequents.map(len).eq(1)].copy()
    result['antecedents'] = result.antecedents.map(lambda items: next(iter(items)))
    result['consequents'] = result.consequents.map(lambda items: next(iter(items)))
    return validate_rules(result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=Path('app/rules.csv'))
    args = parser.parse_args()
    validate_rules(pd.read_csv(args.source))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix('.pending')
    shutil.copyfile(args.source, temporary)
    temporary.replace(args.output)
    print(f'Validated and promoted {args.source} to {args.output}')


if __name__ == '__main__':
    main()
