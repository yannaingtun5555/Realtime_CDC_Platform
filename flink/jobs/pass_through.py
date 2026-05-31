from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema, KafkaOffsetsInitializer

env = StreamExecutionEnvironment.get_execution_environment()
source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topic_pattern("cdc\\.inventory\\.customers") \
    .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()
stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "source")
sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("internal.capture")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .build()
stream.sink_to(sink)
env.execute("PassThrough")
