# Spark historical bootstrap

The historical Instacart data is the system's cold-start prior. The batch job
validates the six source tables, preserves each order's product array, mines
one-item directional FP-Growth rules, and trains implicit-feedback ALS
candidates when the selected slice has enough users and products. Future live
decisions consume these candidates; Redis is only a serving cache.

## Reproducible Docker run

Docker is the recommended local path because PySpark requires a compatible
JVM. The runner builds `mba-spark:4.0.1` from the official Apache Spark image
the first time, adding only NumPy for Spark ML:

```bash
./scripts/run_spark_bootstrap.sh \
  --mode sample \
  --sample-orders 10000 \
  --min-support 0.01 \
  --min-confidence 0.10
```

The input defaults to `InstacartMBA`. Set `INSTACART_DATA_DIR` when the six
CSV files live elsewhere. `SPARK_IMAGE` can select an already prepared image.

For the complete dataset, choose a unique run ID and full mode:

```bash
./scripts/run_spark_bootstrap.sh \
  --mode full \
  --run-id final-full-20261021 \
  --min-support 0.01 \
  --min-confidence 0.10 \
  --num-partitions 16 \
  --als-rank 32 \
  --als-max-iter 10 \
  --als-top-k 50
```

The command refuses to overwrite an existing run directory. Outputs are under
`data/processed/bootstrap/<run-id>/` and include:

- `baskets.parquet` and `baskets.json`, retaining the full `items` arrays;
- `rules.parquet` and `rules.json`, retaining `antecedent` and `consequent`
  arrays after explicitly filtering to one-item directions;
- `rules_serving.csv`, a catalog-ID rule snapshot with display names, accepted
  by both the marketplace and Redis loader;
- `als_candidates.parquet` and `als_candidates.json` when ALS can train;
- `validation.json` and `manifest.json` for machine-readable evidence.

The manifest records source paths and byte sizes, every parameter, validation
metrics, row/artifact counts, Spark metadata, UTC timestamps, duration, ALS
status, and portable output locations relative to the manifest directory. In
sample mode, foreign-key coverage spans every source row; other order and fact
checks cover the selected slice while dimension validation covers the full
dimension tables. Full mode validates every row for every check.

## Publish a mined rule snapshot

After a successful run, use the manifest's `rules_serving_csv` output with the
existing serving adapter:

```bash
uv run --no-sync python -m pipeline.load_rules \
  --rules data/processed/bootstrap/<run-id>/rules_serving.csv
```

This preserves the separation between Spark as the analytical engine and
Redis as the low-latency serving layer.

To run the complete marketplace, simulator, and coupon engine from the same
Spark snapshot, pass the serving artifact to the launcher:

```bash
RULES_PATH=data/processed/bootstrap/<run-id>/rules_serving.csv ./start.sh
```

Flask reads the artifact's `run_id` as the adaptive candidate
`snapshot_version`; `pipeline.load_rules` and the live simulator consume that
same `RULES_PATH` environment variable.

## Local Spark installation

Developers who already have Java 17 can use an isolated Spark environment:

```bash
uv venv .spark-venv --python 3.12
uv pip install --python .spark-venv -r requirements-analytics.txt
.spark-venv/bin/python -m pipeline.spark_bootstrap --help
```

PySpark is intentionally not part of the default project dependency set, so
ordinary application and test environments do not download it.

## Acceptance fixture

The committed miniature six-table dataset provides a fast, deterministic
artifact-seam check:

```bash
./scripts/run_spark_bootstrap.sh \
  --input-dir tests/fixtures/instacart-mini \
  --mode sample --sample-orders 5 \
  --min-support 0.4 --min-confidence 0.5 \
  --num-partitions 1 \
  --als-rank 2 --als-max-iter 2 --als-top-k 2 \
  --run-id acceptance-mini
```

The fixture yields five baskets, two one-item directional rules, and a trained
ALS export. `tests/test_spark_bootstrap_cli.py` verifies those results solely
through the CLI and generated artifacts; it skips when Java/PySpark are not
installed in the host test environment.
