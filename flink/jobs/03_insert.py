#!/usr/bin/env python3
"""Stage 3: Kafka internal.enriched -> Iceberg table on MinIO (PyFlink Table API)."""
import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from job_utils import ICEBERG_JAR, JSON_JAR, KAFKA_CONNECTOR_JARS, load_job_config

JOB_NAME = "CDC Iceberg Sink Job"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg-warehouse/warehouse")
ICEBERG_DATABASE = os.getenv("ICEBERG_DATABASE", "cdc")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "cdc_events")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")


def _configure_s3(table_env: StreamTableEnvironment) -> None:
    cfg = table_env.get_config()
    cfg.set("fs.s3a.endpoint", S3_ENDPOINT)
    cfg.set("fs.s3a.path.style.access", "true")
    cfg.set("fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    cfg.set("fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin123"))
    cfg.set("fs.s3a.connection.ssl.enabled", "false")
    cfg.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def main() -> None:
    job = load_job_config(JOB_NAME)
    input_topic = job["input_topic"]

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(job.get("checkpoint_interval", 10000))
    env.set_parallelism(job.get("parallelism", 2))

    table_env = StreamTableEnvironment.create(env)
    pipeline_jars = ";".join(KAFKA_CONNECTOR_JARS + [ICEBERG_JAR, JSON_JAR])
    table_env.get_config().set("pipeline.jars", pipeline_jars)
    table_env.get_config().set("pipeline.name", JOB_NAME)
    table_env.get_config().set("execution.runtime-mode", "streaming")
    _configure_s3(table_env)

    table_env.execute_sql(
        f"""
        CREATE CATALOG iceberg_hadoop WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hadoop',
            'warehouse' = '{ICEBERG_WAREHOUSE}'
        )
        """
    )
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS iceberg_hadoop.{ICEBERG_DATABASE}")

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS iceberg_hadoop.{ICEBERG_DATABASE}.{ICEBERG_TABLE} (
            db_name STRING,
            table_name STRING,
            operation STRING,
            op_type STRING,
            record_key STRING,
            event_ts_ms BIGINT,
            ingest_ts_ms BIGINT,
            is_delete BOOLEAN,
            payload STRING
        ) WITH (
            'format-version' = '2'
        )
        """
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE kafka_enriched_source (
            db_name STRING,
            table_name STRING,
            operation STRING,
            op_type STRING,
            record_key STRING,
            event_ts_ms BIGINT,
            ingest_ts_ms BIGINT,
            is_delete BOOLEAN,
            payload STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{input_topic}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP}',
            'properties.group.id' = 'flink-iceberg-sink',
            'scan.startup.mode' = 'earliest',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
        """
    )

    # Async submit for remote cluster — do NOT call result.wait() (blocks flink run -d).
    table_env.execute_sql(
        f"""
        INSERT INTO iceberg_hadoop.{ICEBERG_DATABASE}.{ICEBERG_TABLE}
        SELECT
            db_name,
            table_name,
            operation,
            op_type,
            record_key,
            event_ts_ms,
            ingest_ts_ms,
            is_delete,
            payload
        FROM kafka_enriched_source
        """
    )
    print(f"Submitted {JOB_NAME} -> iceberg_hadoop.{ICEBERG_DATABASE}.{ICEBERG_TABLE}")


if __name__ == "__main__":
    main()
