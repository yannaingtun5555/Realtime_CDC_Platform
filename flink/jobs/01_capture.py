#!/usr/bin/env python3
import json
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import FilterFunction, MapFunction

KAFKA_BOOTSTRAP = "kafka:9092"
JOB_NAME = "CDC Ingestion Job"
CONFIG_PATH = "/opt/flink/config.json"

def load_job_config():
    with open(CONFIG_PATH, 'r') as f:
        full_config = json.load(f)
    for job in full_config.get("jobs", []):
        if job.get("name") == JOB_NAME:
            return job
    raise ValueError(f"Job {JOB_NAME} not found in config.json")

class NonNullFilter(FilterFunction):
    def filter(self, value: str) -> bool:
        return value is not None

class CDCEnrichment(MapFunction):
    def map(self, value: str):
        try:
            event = json.loads(value)
            
            # Filter out non-CDC heartbeats
            if "op" not in event or "source" not in event:
                return None
            
            source = event.get("source", {})
            after = event.get("after")
            before = event.get("before")
            
            enriched = {
                "db_name": source.get("db", "unknown"),
                "table_name": source.get("table", "unknown"),
                "operation": event.get("op"),
                "before": before if before else {},
                "after": after if after else {},
                "ts_ms": event.get("ts_ms", 0),
                "source_ts_ms": source.get("ts_ms", 0),
            }
            return json.dumps(enriched)
        except Exception:
            return None

def main():
    job_config = load_job_config()
    input_pattern = job_config.get("input_pattern", r"cdc\..*")
    output_topic = job_config["output_topic"]

    # Source configuration
    source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_topic_pattern(input_pattern) \
        .set_group_id("flink-cdc-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(job_config.get("checkpoint_interval", 10000))
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

    # Pipeline: Source -> Enrich -> Filter None -> Sink
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka CDC Source")
    
    enriched = stream.map(CDCEnrichment(), output_type=Types.STRING()) \
                     .filter(NonNullFilter()) \
                     .name("Enrichment")

    # Sink configuration
    sink = KafkaSink.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP) \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic(output_topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()
        
    enriched.sink_to(sink).name(f"Write to {output_topic}")

    env.execute(JOB_NAME)

if __name__ == "__main__":
    main()