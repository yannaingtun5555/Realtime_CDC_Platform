#!/usr/bin/env python3
import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import MapFunction

KAFKA_BOOTSTRAP = "kafka:9092"
SOURCE_TOPIC_PATTERN = r"cdc\..*"
INTERNAL_TOPIC = "internal.capture"

class CDCEnrichment(MapFunction):
    def map(self, value: str):
        try:
            event = json.loads(value)
            payload = event.get("payload", {})
            source = payload.get("source", {})
            enriched = {
                "db_name": source.get("db", "unknown"),
                "table_name": source.get("table", "unknown"),
                "operation": payload.get("op", "unknown"),
                "before": payload.get("before"),
                "after": payload.get("after"),
                "ts_ms": payload.get("ts_ms", 0),
                "source_ts_ms": source.get("ts_ms", 0),
            }
            return json.dumps(enriched)
        except Exception as e:
            print(f"ERROR: {e}, raw: {value[:200]}")
            return ""

def main():
    env = StreamExecutionEnvironment.get_execution_environment()

    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topic_pattern(SOURCE_TOPIC_PATTERN) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka CDC Source")

    enriched = stream.map(CDCEnrichment(), output_type=Types.STRING()) \
                     .name("Enrichment")

    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(INTERNAL_TOPIC)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    enriched.sink_to(sink).name("Write to internal.capture")
    env.execute("CDC Ingestion Job")

if __name__ == "__main__":
    main()
