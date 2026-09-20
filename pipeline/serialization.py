"""Kafka serializer implementations compatible with kafka-python 3.x."""

from __future__ import annotations

import json
from typing import Any

from kafka.serializer import Deserializer, Serializer


class JsonSerializer(Serializer):
    def serialize(self, topic: str, headers: list[tuple[str, bytes]], data: Any) -> bytes:
        return json.dumps(data, separators=(",", ":")).encode("utf-8")


class JsonDeserializer(Deserializer):
    def deserialize(self, topic: str, headers: list[tuple[str, bytes]], data: bytes | None) -> Any:
        return json.loads(data.decode("utf-8")) if data is not None else None
