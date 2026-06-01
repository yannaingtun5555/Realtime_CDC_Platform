#!/usr/bin/env python3
"""Stage 1: normalize Debezium CDC events into internal.capture."""
import json

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import FilterFunction, MapFunction

from job_utils import (
    build_execution_env,
    kafka_sink,
    kafka_source,
    load_job_config,
    read_stream,
)

JOB_NAME = "CDC Capture Job"


class NonNullFilter(FilterFunction):
    def filter(self, value: str) -> bool:
        return value is not None


class CaptureNormalize(MapFunction):
    def map(self, value: str):
        try:
            event = json.loads(value)
            if "op" not in event or "source" not in event:
                return None
            source = event.get("source", {})
            return json.dumps(
                {
                    "db_name": source.get("db", "unknown"),
                    "table_name": source.get("table", "unknown"),
                    "operation": event.get("op"),
                    "before": event.get("before") or {},
                    "after": event.get("after") or {},
                    "ts_ms": event.get("ts_ms", 0),
                    "source_ts_ms": source.get("ts_ms", 0),
                }
            )
        except (json.JSONDecodeError, TypeError):
            return None


def main() -> None:
    job = load_job_config(JOB_NAME)
    env = build_execution_env(job.get("checkpoint_interval", 10000))
    source = kafka_source(
        group_id="flink-cdc-capture",
        pattern=job.get("input_pattern", r"cdc\..*"),
    )
    stream = read_stream(env, source, "Debezium CDC Source")
    captured = (
        stream.map(CaptureNormalize(), output_type=Types.STRING())
        .filter(NonNullFilter())
        .name("Capture Normalize")
    )
    captured.sink_to(kafka_sink(job["output_topic"])).name(
        f"Kafka Sink {job['output_topic']}"
    )
    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
