"""Versioned Spark bootstrap for the Instacart historical prior.

The command in this module is the public interface.  It deliberately writes
both Parquet (for downstream jobs) and newline-delimited JSON (for inspection
and lightweight consumers), with a manifest that makes every run auditable.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys
import time
from typing import Any
from uuid import uuid4

from pyspark import StorageLevel
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.recommendation import ALS
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


SCHEMA_VERSION = "1.0"
TABLE_FILES = {
    "aisles": "aisles.csv",
    "departments": "departments.csv",
    "products": "products.csv",
    "orders": "orders.csv",
    "order_products_prior": "order_products__prior.csv",
    "order_products_train": "order_products__train.csv",
}

SCHEMAS = {
    "aisles": StructType(
        [StructField("aisle_id", IntegerType(), False), StructField("aisle", StringType(), False)]
    ),
    "departments": StructType(
        [
            StructField("department_id", IntegerType(), False),
            StructField("department", StringType(), False),
        ]
    ),
    "products": StructType(
        [
            StructField("product_id", IntegerType(), False),
            StructField("product_name", StringType(), False),
            StructField("aisle_id", IntegerType(), False),
            StructField("department_id", IntegerType(), False),
        ]
    ),
    "orders": StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("eval_set", StringType(), False),
            StructField("order_number", IntegerType(), False),
            StructField("order_dow", IntegerType(), False),
            StructField("order_hour_of_day", IntegerType(), False),
            StructField("days_since_prior_order", DoubleType(), nullable=True),
        ]
    ),
    "order_products_prior": StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("add_to_cart_order", IntegerType(), False),
            StructField("reordered", IntegerType(), False),
        ]
    ),
    "order_products_train": StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("add_to_cart_order", IntegerType(), False),
            StructField("reordered", IntegerType(), False),
        ]
    ),
}

PRIMARY_KEYS = {
    "aisles": ["aisle_id"],
    "departments": ["department_id"],
    "products": ["product_id"],
    "orders": ["order_id"],
    "order_products_prior": ["order_id", "product_id"],
    "order_products_train": ["order_id", "product_id"],
}

REQUIRED_COLUMNS = {
    name: [field.name for field in schema.fields if not field.nullable]
    for name, schema in SCHEMAS.items()
}


@dataclass(frozen=True)
class BootstrapConfig:
    input_dir: str
    output_dir: str
    run_id: str
    mode: str
    sample_orders: int
    min_support: float
    min_confidence: float
    num_partitions: int
    spark_master: str
    validation_policy: str
    skip_als: bool
    als_rank: int
    als_max_iter: int
    als_reg_param: float
    als_top_k: int
    als_seed: int
    sample_users: int = 0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_tables(spark: SparkSession, input_dir: Path) -> dict[str, DataFrame]:
    tables: dict[str, DataFrame] = {}
    for table, filename in TABLE_FILES.items():
        path = input_dir / filename
        if not path.is_file():
            raise FileNotFoundError(f"Missing required Instacart table: {path}")
        tables[table] = (
            spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            # The published products table uses RFC-style doubled quotes in
            # the `8\" Pecan Pie` row. Spark defaults to backslash escaping,
            # so the quote escape must be explicit to keep that row aligned.
            .option("escape", '"')
            .schema(SCHEMAS[table])
            .csv(str(path))
        )
    return tables


def _source_metadata(input_dir: Path) -> dict[str, dict[str, Any]]:
    metadata = {}
    for table, filename in TABLE_FILES.items():
        source = (input_dir / filename).resolve()
        stat = source.stat()
        metadata[table] = {
            "path": str(source),
            "size_bytes": stat.st_size,
            "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        }
    return metadata


def _bounded_tables(tables: dict[str, DataFrame], config: BootstrapConfig) -> dict[str, DataFrame]:
    if config.mode == "full":
        return tables

    if config.sample_users:
        # Preserve longitudinal history instead of selecting unrelated order IDs.
        selected_users = (tables['orders'].select('user_id').distinct()
                          .orderBy(F.xxhash64('user_id')).limit(config.sample_users))
        selected_orders = tables['orders'].join(F.broadcast(selected_users), 'user_id', 'inner')
        selected_order_ids = selected_orders.select('order_id')
        return {**tables, 'orders': selected_orders,
                'order_products_prior': tables['order_products_prior'].join(selected_order_ids, 'order_id', 'inner'),
                'order_products_train': tables['order_products_train'].join(selected_order_ids, 'order_id', 'inner')}

    selected_orders = (
        tables["orders"]
        .orderBy(F.col("order_id").asc_nulls_last())
        .limit(config.sample_orders)
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    selected_order_ids = selected_orders.select("order_id")
    return {
        **tables,
        "orders": selected_orders,
        "order_products_prior": tables["order_products_prior"].join(
            F.broadcast(selected_order_ids), "order_id", "inner"
        ),
        "order_products_train": tables["order_products_train"].join(
            F.broadcast(selected_order_ids), "order_id", "inner"
        ),
    }


def _condition_count(frame: DataFrame, condition: Any) -> int:
    return frame.where(condition).count()


def _validate(
    tables: dict[str, DataFrame], source_tables: dict[str, DataFrame]
) -> dict[str, Any]:
    row_counts = {name: frame.count() for name, frame in tables.items()}
    nulls: dict[str, dict[str, int]] = {}
    invalid_identifiers: dict[str, dict[str, int]] = {}
    duplicates: dict[str, int] = {}

    identifier_columns = {
        "aisles": ["aisle_id"],
        "departments": ["department_id"],
        "products": ["product_id", "aisle_id", "department_id"],
        "orders": ["order_id", "user_id"],
        "order_products_prior": ["order_id", "product_id"],
        "order_products_train": ["order_id", "product_id"],
    }
    for name, frame in tables.items():
        if REQUIRED_COLUMNS[name]:
            row = frame.agg(
                *[
                    F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
                    for column in REQUIRED_COLUMNS[name]
                ]
            ).first()
            nulls[name] = {column: int(row[column] or 0) for column in REQUIRED_COLUMNS[name]}
        invalid_identifiers[name] = {
            column: _condition_count(frame, F.col(column) <= 0)
            for column in identifier_columns[name]
        }
        keys = PRIMARY_KEYS[name]
        duplicates[name] = (
            frame.groupBy(*keys).count().where(F.col("count") > 1).agg(
                F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("duplicate_rows")
            ).first()["duplicate_rows"]
        )

    orders = tables["orders"]
    products = tables["products"]
    aisles = tables["aisles"]
    departments = tables["departments"]
    prior = tables["order_products_prior"]
    train = tables["order_products_train"]

    source_orders = source_tables["orders"]
    source_products = source_tables["products"]
    source_aisles = source_tables["aisles"]
    source_departments = source_tables["departments"]
    source_prior = source_tables["order_products_prior"]
    source_train = source_tables["order_products_train"]
    foreign_keys = {
        "products_to_aisles": source_products.join(
            source_aisles.select("aisle_id"), "aisle_id", "left_anti"
        ).count(),
        "products_to_departments": source_products.join(
            source_departments.select("department_id"), "department_id", "left_anti"
        ).count(),
        "prior_to_orders": source_prior.join(
            source_orders.select("order_id"), "order_id", "left_anti"
        ).count(),
        "prior_to_products": source_prior.join(
            source_products.select("product_id"), "product_id", "left_anti"
        ).count(),
        "train_to_orders": source_train.join(
            source_orders.select("order_id"), "order_id", "left_anti"
        ).count(),
        "train_to_products": source_train.join(
            source_products.select("product_id"), "product_id", "left_anti"
        ).count(),
    }
    domains = {
        "orders.invalid_eval_set": _condition_count(
            orders, F.col("eval_set").isNotNull() & ~F.col("eval_set").isin("prior", "train", "test")
        ),
        "orders.invalid_order_number": _condition_count(orders, F.col("order_number") <= 0),
        "orders.invalid_order_dow": _condition_count(
            orders, F.col("order_dow").isNotNull() & ~F.col("order_dow").between(0, 6)
        ),
        "orders.invalid_order_hour": _condition_count(
            orders,
            F.col("order_hour_of_day").isNotNull() & ~F.col("order_hour_of_day").between(0, 23),
        ),
        "orders.invalid_days_since_prior": _condition_count(
            orders,
            F.col("days_since_prior_order").isNotNull()
            & ~F.col("days_since_prior_order").between(0.0, 30.0),
        ),
        "prior.invalid_reordered": _condition_count(
            prior, F.col("reordered").isNotNull() & ~F.col("reordered").isin(0, 1)
        ),
        "train.invalid_reordered": _condition_count(
            train, F.col("reordered").isNotNull() & ~F.col("reordered").isin(0, 1)
        ),
        "prior.invalid_add_to_cart_order": _condition_count(prior, F.col("add_to_cart_order") <= 0),
        "train.invalid_add_to_cart_order": _condition_count(train, F.col("add_to_cart_order") <= 0),
        "prior.order_eval_mismatch": prior.join(
            orders.where(F.col("eval_set") != "prior").select("order_id"), "order_id", "inner"
        ).count(),
        "train.order_eval_mismatch": train.join(
            orders.where(F.col("eval_set") != "train").select("order_id"), "order_id", "inner"
        ).count(),
    }
    violation_count = sum(sum(values.values()) for values in nulls.values())
    violation_count += sum(sum(values.values()) for values in invalid_identifiers.values())
    violation_count += sum(duplicates.values()) + sum(foreign_keys.values()) + sum(domains.values())
    return {
        "passed": violation_count == 0,
        "violation_count": int(violation_count),
        "scope": (
            "foreign-key coverage is source-wide; other fact and order checks use the "
            "selected slice in sample mode and all rows in full mode"
        ),
        "row_counts": row_counts,
        "null_required_fields": nulls,
        "invalid_identifiers": invalid_identifiers,
        "duplicate_rows": duplicates,
        "foreign_key_violations": foreign_keys,
        "domain_violations": domains,
    }


def _transactions(tables: dict[str, DataFrame]) -> DataFrame:
    prior = tables["order_products_prior"].select("order_id", "product_id")
    train = tables["order_products_train"].select("order_id", "product_id")
    return (
        prior.unionByName(train)
        .dropDuplicates(["order_id", "product_id"])
        .join(tables["orders"].select("order_id", "user_id", "eval_set"), "order_id", "inner")
    )


def _baskets(transactions: DataFrame) -> DataFrame:
    return (
        transactions.groupBy("order_id", "user_id", "eval_set")
        .agg(F.sort_array(F.collect_set("product_id")).alias("items"))
        .where(F.size("items") > 0)
        .orderBy("order_id")
    )


def _mine_rules(
    baskets: DataFrame,
    products: DataFrame,
    config: BootstrapConfig,
) -> tuple[DataFrame, dict[str, int]]:
    model = FPGrowth(
        itemsCol="items",
        minSupport=config.min_support,
        minConfidence=config.min_confidence,
        numPartitions=config.num_partitions,
    ).fit(baskets.select("items"))
    all_rules = model.associationRules.persist(StorageLevel.MEMORY_AND_DISK)
    one_item = (
        all_rules.where((F.size("antecedent") == 1) & (F.size("consequent") == 1))
        .withColumn("antecedent_product_id", F.element_at("antecedent", 1))
        .withColumn("consequent_product_id", F.element_at("consequent", 1))
        .where(F.col("antecedent_product_id") != F.col("consequent_product_id"))
    )
    rank = Window.partitionBy("antecedent_product_id", "consequent_product_id").orderBy(
        F.desc("lift"), F.desc("confidence"), F.desc("support"),
        F.asc(F.to_json("antecedent")), F.asc(F.to_json("consequent")),
    )
    names = products.select(
        "product_id", "product_name", "aisle_id", "department_id"
    )
    rules = (
        one_item.withColumn("_rank", F.row_number().over(rank))
        .where(F.col("_rank") == 1)
        .drop("_rank")
        .join(
            names.select(
                F.col("product_id").alias("antecedent_product_id"),
                F.col("product_name").alias("antecedent_product_name"),
            ),
            "antecedent_product_id",
            "left",
        )
        .join(
            names.select(
                F.col("product_id").alias("consequent_product_id"),
                F.col("product_name").alias("consequent_product_name"),
            ),
            "consequent_product_id",
            "left",
        )
        .withColumn("run_id", F.lit(config.run_id))
        .select(
            "run_id",
            "antecedent",
            "consequent",
            "antecedent_product_id",
            "antecedent_product_name",
            "consequent_product_id",
            "consequent_product_name",
            "support",
            "confidence",
            "lift",
        )
        .orderBy("antecedent_product_id", "consequent_product_id")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    counts = {
        "all_association_rules": all_rules.count(),
        "one_item_directional_rules": rules.count(),
    }
    return rules, counts


def _als_candidates(
    transactions: DataFrame,
    products: DataFrame,
    config: BootstrapConfig,
) -> tuple[DataFrame | None, dict[str, Any]]:
    interactions = (
        transactions.groupBy("user_id", "product_id")
        .agg(F.count("order_id").cast("float").alias("strength"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    stats = interactions.agg(
        F.count("*").alias("interactions"),
        F.countDistinct("user_id").alias("users"),
        F.countDistinct("product_id").alias("products"),
    ).first().asDict()
    stats = {key: int(value) for key, value in stats.items()}
    if config.skip_als:
        return None, {"status": "skipped", "reason": "disabled_by_configuration", **stats}
    if stats["users"] < 2 or stats["products"] < 2 or stats["interactions"] < 4:
        return None, {"status": "skipped", "reason": "insufficient_slice", **stats}

    max_seen = interactions.groupBy("user_id").count().agg(F.max("count")).first()[0]
    candidate_pool = min(stats["products"], config.als_top_k + int(max_seen or 0))
    als = ALS(
        userCol="user_id",
        itemCol="product_id",
        ratingCol="strength",
        rank=config.als_rank,
        maxIter=config.als_max_iter,
        regParam=config.als_reg_param,
        implicitPrefs=True,
        coldStartStrategy="drop",
        nonnegative=True,
        seed=config.als_seed,
    )
    model = als.fit(interactions)
    scored = (
        model.recommendForAllUsers(candidate_pool)
        .select("user_id", F.posexplode("recommendations").alias("model_rank", "recommendation"))
        .select(
            "user_id",
            F.col("recommendation.product_id").alias("product_id"),
            F.col("recommendation.rating").cast("double").alias("score"),
            "model_rank",
        )
        .join(interactions.select("user_id", "product_id"), ["user_id", "product_id"], "left_anti")
    )
    top_k = Window.partitionBy("user_id").orderBy(
        F.desc("score"), F.asc("product_id")
    )
    candidates = (
        scored.withColumn("rank", F.row_number().over(top_k))
        .where(F.col("rank") <= config.als_top_k)
        .join(products.select("product_id", "product_name"), "product_id", "left")
        .withColumn("run_id", F.lit(config.run_id))
        .select("run_id", "user_id", "product_id", "product_name", "score", "rank")
        .orderBy("user_id", "rank", "product_id")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    exported = candidates.count()
    return candidates, {
        "status": "trained",
        **stats,
        "exported_candidates": exported,
        "candidate_pool_per_user": candidate_pool,
    }


def _write_frame(frame: DataFrame, path: Path, output_format: str, partitions: int) -> None:
    writer_frame = frame.coalesce(partitions)
    writer_frame.write.mode("errorifexists").format(output_format).save(str(path))


def _write_artifact_pair(
    frame: DataFrame, run_dir: Path, artifact_name: str, partitions: int
) -> dict[str, str]:
    parquet = run_dir / f"{artifact_name}.parquet"
    json_output = run_dir / f"{artifact_name}.json"
    _write_frame(frame, parquet, "parquet", partitions)
    _write_frame(frame, json_output, "json", 1)
    return {
        f"{artifact_name}_parquet": parquet.name,
        f"{artifact_name}_json": json_output.name,
    }


def _write_serving_csv(rules: DataFrame, path: Path) -> None:
    """Write one loadable CSV file rather than a Spark part-file directory."""

    temporary = path.parent / f".{path.name}.spark"
    serving = rules.select(
        "antecedent_product_id",
        "consequent_product_id",
        F.col("antecedent_product_name").alias("antecedents"),
        F.col("consequent_product_name").alias("consequents"),
        "support",
        "confidence",
        "lift",
        "run_id",
    )
    serving.coalesce(1).write.mode("errorifexists").option("header", True).csv(str(temporary))
    parts = list(temporary.glob("part-*.csv"))
    if len(parts) != 1:
        raise RuntimeError(f"Expected one serving CSV part, found {len(parts)}")
    parts[0].replace(path)
    shutil.rmtree(temporary)


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def run(config: BootstrapConfig) -> Path:
    started_at = _utc_now()
    started = time.monotonic()
    input_dir = Path(config.input_dir).resolve()
    run_dir = Path(config.output_dir).resolve() / config.run_id
    if run_dir.exists():
        raise FileExistsError(f"Run directory already exists: {run_dir}")
    run_dir.mkdir(parents=True)

    spark = None
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": config.run_id,
        "status": "running",
        "started_at": started_at,
        "parameters": asdict(config),
        "sources": {},
        "spark": {},
        "outputs": {},
        "output_base": "manifest_directory",
    }
    try:
        spark = (
            SparkSession.builder.appName(f"mba-historical-bootstrap-{config.run_id}")
            .master(config.spark_master)
            .config("spark.sql.shuffle.partitions", str(config.num_partitions))
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        manifest["spark"] = {
            "version": spark.version,
            "application_id": spark.sparkContext.applicationId,
            "master": spark.sparkContext.master,
        }
        manifest["sources"] = _source_metadata(input_dir)
        source_tables = _read_tables(spark, input_dir)
        tables = _bounded_tables(source_tables, config)
        validation = _validate(tables, source_tables)
        manifest["validation"] = validation
        validation_path = run_dir / "validation.json"
        _write_json(validation_path, validation)
        manifest["outputs"]["validation_json"] = validation_path.name
        if not validation["passed"] and config.validation_policy == "fail":
            manifest["status"] = "validation_failed"
            raise ValueError(
                f"Input validation found {validation['violation_count']} violation(s); "
                f"see {validation_path}"
            )

        transactions = _transactions(tables).persist(StorageLevel.MEMORY_AND_DISK)
        baskets = _baskets(transactions).persist(StorageLevel.MEMORY_AND_DISK)
        basket_count = baskets.count()
        if basket_count == 0:
            raise ValueError("No non-empty order baskets were available for FP-Growth")

        manifest["outputs"].update(
            _write_artifact_pair(baskets, run_dir, "baskets", config.num_partitions)
        )

        rules, rule_counts = _mine_rules(baskets, tables["products"], config)
        rules_serving_csv = run_dir / "rules_serving.csv"
        manifest["outputs"].update(
            _write_artifact_pair(rules, run_dir, "rules", config.num_partitions)
        )
        _write_serving_csv(rules, rules_serving_csv)
        manifest["outputs"]["rules_serving_csv"] = rules_serving_csv.name

        candidates, als_status = _als_candidates(transactions, tables["products"], config)
        manifest["als"] = als_status
        if candidates is not None:
            manifest["outputs"].update(
                _write_artifact_pair(
                    candidates, run_dir, "als_candidates", config.num_partitions
                )
            )

        manifest["counts"] = {
            **validation["row_counts"],
            "selected_orders": validation["row_counts"]["orders"],
            "transactions": transactions.count(),
            "baskets": basket_count,
            **rule_counts,
        }
        manifest["status"] = "succeeded"
        return run_dir / "manifest.json"
    except Exception as exc:
        if manifest["status"] == "running":
            manifest["status"] = "failed"
        manifest["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        manifest["finished_at"] = _utc_now()
        manifest["duration_seconds"] = round(time.monotonic() - started, 3)
        try:
            _write_json(run_dir / "manifest.json", manifest)
        finally:
            if spark is not None:
                spark.stop()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Mine a versioned historical rule snapshot and implicit-feedback candidates with Spark."
    )
    parser.add_argument("--input-dir", default="InstacartMBA")
    parser.add_argument("--output-dir", default="data/processed/bootstrap")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--mode", choices=("sample", "full"), default="sample")
    parser.add_argument("--sample-orders", type=int, default=10_000)
    parser.add_argument('--sample-users', type=int, default=0, help='Sample complete shopper histories; overrides sample-orders')
    parser.add_argument("--min-support", type=float, default=0.01)
    parser.add_argument("--min-confidence", type=float, default=0.1)
    parser.add_argument("--num-partitions", type=int, default=2)
    parser.add_argument("--spark-master", default="local[*]")
    parser.add_argument("--validation-policy", choices=("fail", "warn"), default="fail")
    parser.add_argument("--skip-als", action="store_true")
    parser.add_argument("--als-rank", type=int, default=16)
    parser.add_argument("--als-max-iter", type=int, default=10)
    parser.add_argument("--als-reg-param", type=float, default=0.1)
    parser.add_argument("--als-top-k", type=int, default=20)
    parser.add_argument("--als-seed", type=int, default=42)
    return parser


def _valid_run_id(value: str) -> str:
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
    if not value or value in {".", ".."} or any(character not in allowed for character in value):
        raise ValueError("run-id may contain only letters, digits, dash, underscore, and dot")
    return value


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.sample_orders <= 0:
        raise SystemExit("--sample-orders must be positive")
    if args.sample_users < 0:
        raise SystemExit('--sample-users cannot be negative')
    if not 0 < args.min_support <= 1 or not 0 <= args.min_confidence <= 1:
        raise SystemExit("support must be in (0, 1] and confidence in [0, 1]")
    run_id = _valid_run_id(
        args.run_id or f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}"
    )
    config = BootstrapConfig(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        run_id=run_id,
        mode=args.mode,
        sample_orders=args.sample_orders,
        min_support=args.min_support,
        min_confidence=args.min_confidence,
        num_partitions=args.num_partitions,
        spark_master=args.spark_master,
        validation_policy=args.validation_policy,
        skip_als=args.skip_als,
        als_rank=args.als_rank,
        als_max_iter=args.als_max_iter,
        als_reg_param=args.als_reg_param,
        als_top_k=args.als_top_k,
        als_seed=args.als_seed,
        sample_users=args.sample_users,
    )
    try:
        manifest = run(config)
    except Exception as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps({"status": "succeeded", "manifest": str(manifest)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
