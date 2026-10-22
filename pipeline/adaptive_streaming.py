"""Spark Structured Streaming entry point for adaptive feature updates.

PySpark is imported only when a stream is built, so the Flask application and
unit tests do not require a Spark runtime.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from redis import Redis

from pipeline.adaptive import AdaptiveRankingService
from pipeline.adaptive_redis import create_redis_adaptive_service
from pipeline.contracts import validate_event
from pipeline.topics import EVENTS_TOPIC


EventConsumer = Callable[[dict[str, Any], int], Any]
RejectedConsumer = Callable[[Mapping[str, Any], int, str], Any]


class JsonlDeadLetterSink:
    """Append rejected streaming rows without replaying a poison micro-batch."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def __call__(self, values: Mapping[str, Any], batch_id: int, error: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        record = {"batch_id": batch_id, "error": error, "row": dict(values)}
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, default=str, sort_keys=True) + "\n")


class AdaptiveForeachBatch:
    """Testable driver-side boundary between Spark batches and live state."""

    def __init__(
        self,
        consume: EventConsumer,
        on_rejected: RejectedConsumer | None = None,
    ) -> None:
        self.consume = consume
        self.on_rejected = on_rejected
        self.rejected_count = 0

    def __call__(self, batch_df: Any, batch_id: int) -> None:
        for row in batch_df.toLocalIterator():
            values = row.asDict(recursive=True)
            try:
                if "event_json" in values:
                    payload = json.loads(values["event_json"])
                else:
                    payload = {
                        key: _json_value(value)
                        for key, value in values.items()
                        if key != "event_time_timestamp" and value is not None
                    }
                payload = validate_event(_canonical_adaptive_payload(payload))
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self.rejected_count += 1
                if self.on_rejected is not None:
                    self.on_rejected(values, int(batch_id), str(exc))
                continue
            self.consume(payload, int(batch_id))


def _canonical_adaptive_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Prefer stable catalog IDs while retaining names in the raw event."""
    canonical = dict(payload)
    catalog_product_id = canonical.get("catalog_product_id")
    if isinstance(catalog_product_id, (int, str)) and not isinstance(catalog_product_id, bool):
        canonical["product_id"] = str(catalog_product_id)
    catalog_product_ids = canonical.get("catalog_product_ids")
    if isinstance(catalog_product_ids, list):
        canonical["product_ids"] = [str(product_id) for product_id in catalog_product_ids]
    if canonical.get('event_type') == 'cart_synchronized':
        canonical['cart'] = [{**item, 'product_id': str(item.get('catalog_product_id', item['product_id']))}
                             for item in canonical['cart']]
    return canonical


def consume_with(service: AdaptiveRankingService) -> EventConsumer:
    def consume(payload: dict[str, Any], batch_id: int) -> None:
        del batch_id
        result = service.observe_event(payload)
        if result.status == "state_unavailable":
            raise RuntimeError("adaptive state unavailable; retry the micro-batch")

    return consume


def kafka_event_schema() -> Any:
    """Return the explicit JSON schema used to parse Kafka values."""

    try:
        from pyspark.sql.types import (
            ArrayType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
        )
    except ImportError as exc:  # pragma: no cover - depends on optional runtime
        raise RuntimeError("PySpark is required to build the structured stream") from exc

    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_time", StringType(), False),
            StructField("user_id", StringType(), False),
            StructField("session_id", StringType(), False),
            StructField("product_id", StringType(), True),
            StructField("product_ids", ArrayType(StringType(), containsNull=False), True),
            StructField("catalog_product_id", LongType(), True),
            StructField("catalog_product_ids", ArrayType(LongType(), containsNull=False), True),
            StructField("quantity", IntegerType(), True),
            StructField("dwell_seconds", DoubleType(), True),
            StructField("query", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("impression_id", StringType(), True),
            StructField("policy_version", StringType(), True),
            StructField("snapshot_version", StringType(), True),
            StructField("placement", StringType(), True),
            StructField("coupon_id", StringType(), True),
        ]
    )


def build_query(
    spark: Any,
    *,
    bootstrap_servers: str,
    topic: str,
    checkpoint_location: str,
    watermark: str,
    foreach_batch: AdaptiveForeachBatch,
    starting_offsets: str = "earliest",
    available_now: bool = False,
) -> Any:
    """Build (without awaiting) the Kafka-to-feature Structured Streaming query."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:  # pragma: no cover - depends on optional runtime
        raise RuntimeError("PySpark is required to build the structured stream") from exc

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )
    decoded = kafka.select(
        functions.col("value").cast("string").alias("event_json"),
        "topic",
        "partition",
        "offset",
        functions.col("timestamp").alias("kafka_timestamp"),
    )
    parsed = (
        decoded.select(
            "*",
            functions.from_json(
                functions.col("event_json"), kafka_event_schema()
            ).alias("event")
        )
        .withColumn(
            "event_time_timestamp", functions.try_to_timestamp("event.event_time")
        )
        .withColumn(
            "stream_event_time",
            functions.coalesce("event_time_timestamp", "kafka_timestamp"),
        )
        .withColumn(
            "stream_dedup_id",
            functions.coalesce(
                functions.col("event.event_id"),
                functions.concat_ws(":", "topic", "partition", "offset"),
            ),
        )
    )
    routed = (
        parsed.select(
            "event_json",
            "event.*",
            "event_time_timestamp",
            "stream_event_time",
            "stream_dedup_id",
        )
        .withWatermark("stream_event_time", watermark)
        .dropDuplicatesWithinWatermark(["stream_dedup_id"])
    )
    writer = (
        routed.writeStream.outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .foreachBatch(foreach_batch)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start()


def main() -> None:
    parser = argparse.ArgumentParser(description="Update adaptive features from Kafka with Spark")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--topic", default=EVENTS_TOPIC)
    parser.add_argument(
        "--checkpoint-location",
        default=os.getenv("ADAPTIVE_CHECKPOINT", ".runtime/checkpoints/adaptive-features"),
    )
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--starting-offsets", choices=("earliest", "latest"), default="earliest")
    parser.add_argument('--available-now', action='store_true')
    parser.add_argument(
        "--dead-letter-path",
        default=os.getenv("ADAPTIVE_DEAD_LETTER", ".runtime/adaptive-dead-letter.jsonl"),
    )
    args = parser.parse_args()

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - depends on optional runtime
        raise SystemExit("PySpark is required: install the project Spark dependency") from exc

    spark = (SparkSession.builder.appName("mba-live-adaptive-features")
             .config('spark.sql.shuffle.partitions', os.getenv('SPARK_SHUFFLE_PARTITIONS', '2'))
             .config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    redis_url = os.getenv("ADAPTIVE_REDIS_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    redis_client = Redis.from_url(redis_url, decode_responses=True)
    redis_client.ping()
    service = create_redis_adaptive_service(redis_client)
    query = build_query(
        spark,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        checkpoint_location=args.checkpoint_location,
        watermark=args.watermark,
        starting_offsets=args.starting_offsets,
        available_now=args.available_now,
        foreach_batch=AdaptiveForeachBatch(
            consume_with(service), JsonlDeadLetterSink(args.dead_letter_path)
        ),
    )
    try:
        query.awaitTermination()
        print(json.dumps({'status': 'succeeded', 'progress': query.lastProgress}, default=str), flush=True)
    finally:
        spark.stop()


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


if __name__ == "__main__":
    main()
