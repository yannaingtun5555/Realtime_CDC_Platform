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
            source = event.get("source", {})
            db_name = source.get("db", "unknown")
            table_name = source.get("table", "unknown")
            op = event.get("op", "unknown")
            after = event.get("after", {})
            before = event.get("before", {})
            ts_ms = event.get("ts_ms", 0)
            source_ts_ms = source.get("ts_ms", 0)

            enriched = {
                "db_name": db_name,
                "table_name": table_name,
                "operation": op,
                "before": before,
                "after": after,
                "ts_ms": ts_ms,
                "source_ts_ms": source_ts_ms,
            }
            return json.dumps(enriched)
        except Exception as e:
            print(f"Failed to parse: {e}, raw: {value[:200]}")
            return ""   # return empty string to avoid None

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
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
