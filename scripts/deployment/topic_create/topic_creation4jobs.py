#!/usr/bin/env python3
"""
Dynamically creates internal topics with partition count = sum of partitions
of all source CDC topics.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "kafka:9092"
SOURCE_TOPIC_PATTERN = r"^cdc\..*"   # matches all Debezium topics
INTERNAL_TOPICS = ["internal.capture", "internal.enrich"]
PARTITION_FACTOR = 1   # can be adjusted

def get_total_partitions():
    """List all source topics, sum their partition counts."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
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
    return max(total, 1)   # at least 1

def create_internal_topics():
    total = get_total_partitions()
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
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
        admin.create_topics([new_topic])
        logger.info(f"Created topic {topic_name} with {total} partitions.")
    admin.close()

if __name__ == "__main__":
    create_internal_topics()