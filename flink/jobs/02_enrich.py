#!/usr/bin/env python3
"""Stage 2: shallow enrichment from internal.capture to internal.enriched."""
import json
import time

from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import FilterFunction, MapFunction

from job_utils import (
    build_execution_env,
    kafka_sink,
    kafka_source,
    load_job_config,
    read_stream,
)

JOB_NAME = "CDC Enrich Job"

OP_LABELS = {"c": "INSERT", "r": "READ", "u": "UPDATE", "d": "DELETE"}


class NonNullFilter(FilterFunction):
    def filter(self, value: str) -> bool:
        return value is not None


class ShallowEnrich(MapFunction):
    def map(self, value: str):
        try:
            event = json.loads(value)
            operation = event.get("operation")
            if not operation:
                return None
            after = event.get("after") or {}
            before = event.get("before") or {}
            payload = after if operation != "d" else before
            record_id = payload.get("id")
            table_name = event.get("table_name", "unknown")
            record_key = (
                f"{table_name}:{record_id}" if record_id is not None else f"{table_name}:unknown"
            )
            return json.dumps(
                {
                    "db_name": event.get("db_name", "unknown"),
                    "table_name": table_name,
                    "operation": operation,
                    "op_type": OP_LABELS.get(operation, operation.upper()),
                    "record_key": record_key,
                    "event_ts_ms": event.get("ts_ms", 0),
                    "ingest_ts_ms": int(time.time() * 1000),
                    "is_delete": operation == "d",
                    "payload": json.dumps(payload),
                }
            )
        except (json.JSONDecodeError, TypeError):
            return None


def main() -> None:
    job = load_job_config(JOB_NAME)
    env = build_execution_env(job.get("checkpoint_interval", 10000))
    source = kafka_source(
        group_id="flink-cdc-enrich",
        topic=job["input_topic"],
    )
    stream = read_stream(env, source, "Captured CDC Stream")
    enriched = (
        stream.map(ShallowEnrich(), output_type=Types.STRING())
        .filter(NonNullFilter())
        .name("Shallow Enrich")
    )
    enriched.sink_to(kafka_sink(job["output_topic"])).name(
        f"Kafka Sink {job['output_topic']}"
    )
    env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
