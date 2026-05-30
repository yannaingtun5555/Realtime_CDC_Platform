#!/usr/bin/env python3
"""Create internal Kafka topics after Debezium connectors are running."""

import logging
import os
import time
import re
import requests

from kafka.admin import KafkaAdminClient, NewTopic
try:
    from kafka.admin import NewPartitions
except ImportError:
    NewPartitions = None
from kafka.errors import KafkaError, TopicAlreadyExistsError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")
SOURCE_TOPIC_PATTERN = re.compile(r"^cdc\..*")
INTERNAL_TOPICS = ["internal.capture", "internal.enrich"]
PARTITION_FACTOR = int(os.getenv("PARTITION_FACTOR", "1"))
MAX_RETRIES_KAFKA = int(os.getenv("KAFKA_READY_RETRIES", "30"))
MAX_RETRIES_CONNECTOR = int(os.getenv("CONNECTOR_READY_RETRIES", "60"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY_SECONDS", "2"))


def get_admin_client():
    return KafkaAdminClient(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        client_id="cdc-topic-automation",
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=10000,
    )


def wait_for_kafka():
    logger.info("Waiting for Kafka at %s...", BOOTSTRAP_SERVERS)
    for attempt in range(1, MAX_RETRIES_KAFKA + 1):
        admin = None
        try:
            admin = get_admin_client()
            admin.list_topics()
            logger.info("Kafka is ready.")
            return True
        except KafkaError as exc:
            logger.info("Kafka not ready (%s/%s): %s", attempt, MAX_RETRIES_KAFKA, exc)
        finally:
            if admin:
                admin.close()
        time.sleep(RETRY_DELAY)
    logger.error("Kafka did not become ready.")
    return False


def wait_for_connector():
    """Wait for at least one Debezium connector to be in RUNNING state."""
    logger.info("Waiting for a Debezium connector to be RUNNING...")
    for attempt in range(1, MAX_RETRIES_CONNECTOR + 1):
        try:
            resp = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
            if resp.status_code != 200:
                logger.info("Connect API not ready (%s/%s)", attempt, MAX_RETRIES_CONNECTOR)
                time.sleep(RETRY_DELAY)
                continue
            connectors = resp.json()
            if not connectors:
                logger.info("No connectors registered yet (%s/%s)", attempt, MAX_RETRIES_CONNECTOR)
                time.sleep(RETRY_DELAY)
                continue
            # Check status of each connector
            for name in connectors:
                status_resp = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=5)
                if status_resp.status_code == 200:
                    state = status_resp.json().get("connector", {}).get("state")
                    if state == "RUNNING":
                        logger.info("Connector %s is RUNNING.", name)
                        return True
                    else:
                        logger.debug("Connector %s state: %s", name, state)
            logger.info("No RUNNING connector yet (%s/%s)", attempt, MAX_RETRIES_CONNECTOR)
        except Exception as e:
            logger.warning("Error checking connectors: %s", e)
        time.sleep(RETRY_DELAY)
    logger.error("No Debezium connector reached RUNNING state in time.")
    return False


def get_total_partitions():
    """Sum partitions of all source topics (cdc.*)."""
    admin = get_admin_client()
    all_topics = admin.list_topics()
    source_topics = [t for t in all_topics if SOURCE_TOPIC_PATTERN.match(t)]
    if not source_topics:
        raise RuntimeError("No source topics (cdc.*) found after connectors are RUNNING. Cannot determine partition count.")
    total = 0
    for topic in source_topics:
        partitions = admin.describe_topics([topic])[0]['partitions']
        count = len(partitions)
        logger.info("Source topic %s has %s partitions", topic, count)
        total += count
    final = max(total * PARTITION_FACTOR, 1)
    logger.info("Total partitions for internal topics: %s", final)
    return final


def create_or_resize_topic(admin, topic_name, target_partitions):
    """Create topic or expand its partition count."""
    if topic_name in admin.list_topics():
        current = len(admin.describe_topics([topic_name])[0]["partitions"])
        if current >= target_partitions:
            logger.info("Topic %s already has %s partitions (target %s). Skipping.", topic_name, current, target_partitions)
            return
        if NewPartitions is None:
            logger.warning("Cannot resize topic %s because kafka-python lacks NewPartitions. Skipping.", topic_name)
            return
        admin.create_partitions({topic_name: NewPartitions(total_count=target_partitions)})
        logger.info("Expanded topic %s from %s to %s partitions.", topic_name, current, target_partitions)
        return
    new_topic = NewTopic(name=topic_name, num_partitions=target_partitions, replication_factor=1)
    try:
        admin.create_topics([new_topic], validate_only=False)
        logger.info("Created topic %s with %s partitions.", topic_name, target_partitions)
    except TopicAlreadyExistsError:
        logger.info("Topic %s already exists.", topic_name)


def main():
    if not wait_for_kafka():
        raise SystemExit(1)
    if not wait_for_connector():
        raise SystemExit(1)
    # Give a short extra delay to allow source topics to be fully created
    logger.info("Connector is RUNNING. Waiting 5 seconds for source topics to appear...")
    time.sleep(5)

    try:
        total_partitions = get_total_partitions()
    except RuntimeError as e:
        logger.error(e)
        raise SystemExit(1)

    admin = get_admin_client()
    for topic in INTERNAL_TOPICS:
        create_or_resize_topic(admin, topic, total_partitions)
    admin.close()
    logger.info("Internal topics ready.")


if __name__ == "__main__":
    main()