from __future__ import annotations

import json
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys
import csv

import pytest


def _json_rows(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for part in sorted(path.glob("part-*.json"))
        for line in part.read_text(encoding="utf-8").splitlines()
        if line
    ]


def _artifact(manifest_path: Path, manifest: dict, name: str) -> Path:
    return manifest_path.parent / manifest["outputs"][name]


@pytest.mark.skipif(
    not os.getenv('SPARK_TEST_IMAGE') and ((shutil.which("java") is None and not os.environ.get("JAVA_HOME"))
    or importlib.util.find_spec("pyspark") is None),
    reason="Set SPARK_TEST_IMAGE to run the real Spark acceptance test in Docker",
)
def test_batch_cli_publishes_validated_versioned_artifacts(tmp_path: Path):
    source = tmp_path / "source"
    output = tmp_path / "artifacts"
    shutil.copytree(Path(__file__).parent / "fixtures" / "instacart-mini", source)

    prefix = [sys.executable, '-m', 'pipeline.spark_bootstrap']
    if os.getenv('SPARK_TEST_IMAGE'):
        prefix = ['docker', 'run', '--rm', '--user', f'{os.getuid()}:{os.getgid()}',
                  '--env', 'HOME=/tmp', '--env', 'SPARK_USER=mba-runner',
                  '--volume', '/etc/passwd:/etc/passwd:ro', '--volume', '/etc/group:/etc/group:ro',
                  '--volume', f'{Path(__file__).parents[1]}:/workspace',
                  '--volume', f'{tmp_path}:{tmp_path}', '--workdir', '/workspace',
                  os.environ['SPARK_TEST_IMAGE'], '/opt/spark/bin/spark-submit',
                  '--master', 'local[2]', '--conf', 'spark.jars.ivy=/tmp/.ivy2', 'pipeline/spark_bootstrap.py']
    completed = subprocess.run(
        prefix + [
            "--input-dir",
            str(source),
            "--output-dir",
            str(output),
            "--run-id",
            "fixture-run",
            "--mode",
            "sample",
            "--sample-orders",
            "5",
            "--min-support",
            "0.4",
            "--min-confidence",
            "0.5",
            "--num-partitions",
            "1",
            "--als-rank",
            "2",
            "--als-max-iter",
            "2",
            "--als-top-k",
            "2",
            '--spark-master', 'local[2]',
        ],
        cwd=Path(__file__).parents[1],
        text=True,
        capture_output=True,
        timeout=180,
    )

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout.splitlines()[-1])
    manifest_path = Path(result["manifest"])
    assert manifest_path == output / "fixture-run" / "manifest.json"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "1.0"
    assert manifest["run_id"] == "fixture-run"
    assert manifest["status"] == "succeeded"
    assert manifest["parameters"]["mode"] == "sample"
    assert manifest["counts"]["selected_orders"] == 5
    assert manifest["counts"]["baskets"] == 5
    assert manifest["validation"]["passed"] is True
    assert set(manifest["sources"]) == {
        "aisles",
        "departments",
        "products",
        "orders",
        "order_products_prior",
        "order_products_train",
    }
    assert all(source_info["size_bytes"] > 0 for source_info in manifest["sources"].values())

    assert manifest["output_base"] == "manifest_directory"
    baskets = _json_rows(_artifact(manifest_path, manifest, "baskets_json"))
    assert {tuple(row["items"]) for row in baskets} >= {(1, 2), (1, 3), (2, 4)}

    rules = _json_rows(_artifact(manifest_path, manifest, "rules_json"))
    assert rules
    directed_pairs = [(row["antecedent_product_id"], row["consequent_product_id"]) for row in rules]
    assert len(directed_pairs) == len(set(directed_pairs))
    assert all(len(row["antecedent"]) == len(row["consequent"]) == 1 for row in rules)
    assert all(row["antecedent"][0] != row["consequent"][0] for row in rules)
    assert all({"support", "confidence", "lift"} <= row.keys() for row in rules)
    assert manifest["als"]["status"] == "trained"
    assert _artifact(manifest_path, manifest, "als_candidates_parquet").is_dir()
    assert _artifact(manifest_path, manifest, "rules_parquet").is_dir()
    with _artifact(manifest_path, manifest, "rules_serving_csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        serving_rules = list(csv.DictReader(handle))
    assert serving_rules
    assert set(serving_rules[0]) == {
        "antecedent_product_id",
        "consequent_product_id",
        "antecedents",
        "consequents",
        "support",
        "confidence",
        "lift",
        "run_id",
    }
    assert {row["run_id"] for row in serving_rules} == {"fixture-run"}
    assert all(row["antecedent_product_id"].isdigit() for row in serving_rules)
    assert all(row["consequent_product_id"].isdigit() for row in serving_rules)
