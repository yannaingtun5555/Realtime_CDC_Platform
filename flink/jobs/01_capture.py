#!/usr/bin/env python3
"""
Flink Job 1: Ingestion
Consumes all CDC topics (cdc.*), parses Debezium JSON, extracts metadata,
and writes enriched events to internal.capture.
"""

import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy
from pyflink.datastream.functions import MapFunction

# ---------------------------
# Configuration
# ---------------------------
KAFKA_BOOTSTRAP = "kafka:9092"
SOURCE_TOPIC_PATTERN = r"cdc\..*"
INTERNAL_TOPIC = "internal.capture"

# ---------------------------
# Helper function: parse Debezium JSON and add db_name, table_name
# ---------------------------
class CDCEnrichment(MapFunction):
    def map(self, value: str):
        try:
            event = json.loads(value)
            payload = event.get("payload", {})
            source = payload.get("source", {})

            db_name = source.get("db", "unknown")
            table_name = source.get("table", "unknown")
            op = payload.get("op", "unknown")
            # Extract after image (for insert/update) or before (for delete)
            after = payload.get("after", {})
            before = payload.get("before", {})

            # Construct enriched output as JSON string
            enriched = {
                "db_name": db_name,
                "table_name": table_name,
                "operation": op,
                "before": before,
                "after": after,
                "ts_ms": payload.get("ts_ms", 0),
                "source_ts_ms": source.get("ts_ms", 0),
                "original_topic": None  # can be added later
            }
            return json.dumps(enriched)
        except Exception as e:
            # Log error but don't crash the job
            print(f"Failed to parse message: {e}, raw: {value[:200]}")
            return None

# ---------------------------
# Main pipeline
# ---------------------------
def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    # 1. Kafka source with pattern
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topic_pattern(SOURCE_TOPIC_PATTERN) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    kafka_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka CDC Source"
    )

    # 2. Parse and enrich
    enriched_stream = kafka_stream \
        .map(CDCEnrichment()) \
        .filter(lambda x: x is not None) \
        .name("Parse and enrich CDC events")

    # 3. Kafka sink to internal.capture
    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(INTERNAL_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    enriched_stream.sink_to(sink).name("Write to internal.capture")

    # 4. Execute job
    env.execute("CDC Ingestion Job")

if __name__ == "__main__":
    main()
