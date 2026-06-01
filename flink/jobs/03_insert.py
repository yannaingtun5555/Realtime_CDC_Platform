#!/usr/bin/env python3
"""Stage 3: stream enriched CDC events into Iceberg tables on MinIO (PyFlink Table API)."""
import os

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

from job_utils import ICEBERG_JAR, KAFKA_CONNECTOR_JARS, load_job_config

JOB_NAME = "CDC Iceberg Sink Job"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://iceberg-warehouse/warehouse")
ICEBERG_DATABASE = os.getenv("ICEBERG_DATABASE", "cdc")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", "cdc_events")


def main() -> None:
    job = load_job_config(JOB_NAME)
    input_topic = job["input_topic"]

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(job.get("checkpoint_interval", 10000))
    env.set_parallelism(job.get("parallelism", 2))

    table_env = StreamTableEnvironment.create(env)
    pipeline_jars = ";".join(KAFKA_CONNECTOR_JARS + [ICEBERG_JAR])
    table_env.get_config().set("pipeline.jars", pipeline_jars)

    table_env.execute_sql(
        f"""
        CREATE CATALOG iceberg_hadoop WITH (
            'type' = 'iceberg',
            'catalog-type' = 'hadoop',
            'warehouse' = '{ICEBERG_WAREHOUSE}'
        )
        """
    )
    table_env.execute_sql("USE CATALOG iceberg_hadoop")
    table_env.execute_sql(f"CREATE DATABASE IF NOT EXISTS {ICEBERG_DATABASE}")
    table_env.execute_sql(f"USE {ICEBERG_DATABASE}")

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            db_name STRING,
            table_name STRING,
            operation STRING,
            op_type STRING,
            record_key STRING,
            event_ts_ms BIGINT,
            ingest_ts_ms BIGINT,
            is_delete BOOLEAN,
            payload STRING,
            PRIMARY KEY (record_key, event_ts_ms) NOT ENFORCED
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

    result = table_env.execute_sql(
        f"""
        INSERT INTO {ICEBERG_TABLE}
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
    print(f"Iceberg sink job started: {ICEBERG_DATABASE}.{ICEBERG_TABLE}")
    result.wait()


if __name__ == "__main__":
    main()
