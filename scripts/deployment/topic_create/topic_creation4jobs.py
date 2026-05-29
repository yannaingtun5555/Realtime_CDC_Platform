#!/usr/bin/env python3
"""Create the internal Kafka topics used by the Flink jobs."""

import logging
import os
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
SOURCE_TOPIC_PATTERN = r"^cdc\..*"   # matches all Debezium topics
INTERNAL_TOPICS = ["internal.capture", "internal.enrich"]
PARTITION_FACTOR = 1   # can be adjusted
DEFAULT_PARTITIONS = int(os.getenv("DEFAULT_PARTITIONS", "1"))
MAX_RETRIES = int(os.getenv("KAFKA_READY_RETRIES", "30"))
RETRY_DELAY_SECONDS = int(os.getenv("KAFKA_READY_DELAY_SECONDS", "2"))


def get_admin_client():
    return KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="cdc-topic-automation",
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=10000,
    )


def wait_for_kafka():
    """Wait until Kafka accepts admin requests."""
    logger.info("Waiting for Kafka at %s...", BOOTSTRAP_SERVERS)
    for attempt in range(1, MAX_RETRIES + 1):
        admin = None
        try:
            admin = get_admin_client()
            admin.list_topics()
            logger.info("Kafka is ready.")
            return True
        except KafkaError as exc:
            logger.info("Kafka not ready yet (%s/%s): %s", attempt, MAX_RETRIES, exc)
        finally:
            if admin:
                admin.close()
        time.sleep(RETRY_DELAY_SECONDS)

    logger.error("Kafka did not become ready after %s attempts.", MAX_RETRIES)
    return False

def get_total_partitions():
    """List all source topics, sum their partition counts."""
    admin = get_admin_client()
    # Get all topics matching pattern
    all_topics = admin.list_topics()
    source_topics = [t for t in all_topics if t.startswith("cdc.")]
    total = 0
    for topic in source_topics:
        # get partition count for this topic
        partitions = admin.describe_topics([topic])[0]['partitions']
        count = len(partitions)
        logger.info(f"{topic} has {count} partitions")
        total += count
    admin.close()
    logger.info(f"Total partitions to allocate: {total}")
    return max(total * PARTITION_FACTOR, DEFAULT_PARTITIONS)

def create_internal_topics():
    total = get_total_partitions()
    admin = get_admin_client()
    for topic_name in INTERNAL_TOPICS:
        # check if exists
        if topic_name in admin.list_topics():
            logger.info(f"Topic {topic_name} already exists. Skipping creation.")
            continue
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=total,
            replication_factor=1
        )
        try:
            admin.create_topics([new_topic], validate_only=False)
            logger.info(f"Created topic {topic_name} with {total} partitions.")
        except TopicAlreadyExistsError:
            logger.info(f"Topic {topic_name} already exists. Skipping creation.")
    admin.close()

if __name__ == "__main__":
    if wait_for_kafka():
        create_internal_topics()
    else:
        raise SystemExit(1)
