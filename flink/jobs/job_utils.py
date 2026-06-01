#!/usr/bin/env python3
"""Shared helpers for PyFlink CDC jobs."""
from __future__ import annotations

import json
import os
from typing import Any

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/opt/flink/config.json")

KAFKA_CONNECTOR_JARS = [
    "file:///opt/flink/jars/flink-connector-kafka-3.2.0-1.18.jar",
    "file:///opt/flink/jars/kafka-clients-3.4.0.jar",
]
ICEBERG_DIR = "file:///opt/flink/jars/iceberg"
ICEBERG_JAR = f"{ICEBERG_DIR}/iceberg-flink-runtime-1.18-1.6.1.jar"
JSON_JAR = "file:///opt/flink/jars/flink-json-1.18.1.jar"
# Job 03 only — keep out of /opt/flink/lib to avoid LinkageError with Flink CLI (commons-cli).
ICEBERG_HADOOP_JARS = [
    f"{ICEBERG_DIR}/iceberg-flink-runtime-1.18-1.6.1.jar",
    f"{ICEBERG_DIR}/hadoop-common-3.3.4.jar",
    f"{ICEBERG_DIR}/hadoop-hdfs-client-3.3.4.jar",
    f"{ICEBERG_DIR}/hadoop-auth-3.3.4.jar",
    f"{ICEBERG_DIR}/hadoop-aws-3.3.4.jar",
    f"{ICEBERG_DIR}/commons-configuration2-2.10.1.jar",
    f"{ICEBERG_DIR}/stax2-api-4.2.1.jar",
    f"{ICEBERG_DIR}/woodstox-core-6.4.0.jar",
]
TABLE_SINK_JARS = KAFKA_CONNECTOR_JARS + [JSON_JAR] + ICEBERG_HADOOP_JARS


def load_config() -> dict[str, Any]:
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_job_config(job_name: str) -> dict[str, Any]:
    config = load_config()
    for job in config.get("jobs", []):
        if job.get("name") == job_name:
            return job
    raise ValueError(f"Job {job_name!r} not found in {CONFIG_PATH}")


def build_execution_env(checkpoint_interval: int) -> StreamExecutionEnvironment:
    from pyflink.datastream import CheckpointingMode

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(checkpoint_interval)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    return env


def kafka_source(
    group_id: str,
    *,
    topic: str | None = None,
    pattern: str | None = None,
) -> KafkaSource:
    if not topic and not pattern:
        raise ValueError("kafka_source requires topic or pattern")
    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if pattern:
        builder = builder.set_topic_pattern(pattern)
    else:
        builder = builder.set_topics(topic)
    return builder.build()


def kafka_sink(topic: str) -> KafkaSink:
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .build()
    )


def read_stream(env: StreamExecutionEnvironment, source: KafkaSource, name: str):
    return env.from_source(source, WatermarkStrategy.no_watermarks(), name)
