from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

env = StreamExecutionEnvironment.get_execution_environment()

# Disable checkpointing for this test (set interval to 0)


# Add Kafka connector JARs (they are already in /opt/flink/lib, but this ensures they are on the classpath)
env.add_jars(
    "file:///opt/flink/lib/flink-connector-kafka-3.2.0-1.18.jar",
    "file:///opt/flink/lib/kafka-clients-3.2.0.jar"
)

source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("cdc.test.test_db1.public.users") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
stream.print()

env.execute("Test Kafka Consumer")