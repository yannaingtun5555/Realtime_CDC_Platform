#!/usr/bin/env python3
"""
Interactive Kafka topic creator using docker exec (bypasses DNS issues).
Usage: python topic_creation.py
"""

import subprocess
import sys
import os

KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "cdc-kafka")
BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

def run_kafka_command(*args):
    """Run a kafka-topics command inside the Kafka container."""
    cmd = ["docker", "exec", KAFKA_CONTAINER, "kafka-topics", "--bootstrap-server", BOOTSTRAP_SERVER] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result

def create_topic(topic_name=None, partitions=None, replication=None):
    topic_name = (topic_name or os.getenv("TOPIC_NAME") or input("Enter topic name (e.g., cdc.acme.db1.public.users): ")).strip()
    if not topic_name:
        print("Topic name cannot be empty.")
        return False

    try:
        partitions = int(partitions or os.getenv("TOPIC_PARTITIONS") or input("Number of partitions [3]: ") or "3")
        replication = int(replication or os.getenv("TOPIC_REPLICATION_FACTOR") or input("Replication factor [1]: ") or "1")
    except ValueError:
        print("Invalid number, using defaults.")
        partitions, replication = 3, 1

    # Check if topic already exists
    list_result = run_kafka_command("--list")
    existing_topics = set(list_result.stdout.strip().split())
    if topic_name in existing_topics:
        print(f"Topic '{topic_name}' already exists.")
        return True

    # Create topic
    create_result = run_kafka_command(
        "--create", "--topic", topic_name,
        "--partitions", str(partitions),
        "--replication-factor", str(replication)
    )

    if create_result.returncode == 0:
        print(f"Topic '{topic_name}' created successfully.")
        return True
    else:
        print(f"Failed to create topic: {create_result.stderr}")
        return False

if __name__ == "__main__":
    # Verify Kafka container is running
    check = subprocess.run(["docker", "ps", "--filter", f"name={KAFKA_CONTAINER}", "--format", "{{.Names}}"],
                           capture_output=True, text=True)
    if KAFKA_CONTAINER not in check.stdout:
        print(f"Error: Kafka container '{KAFKA_CONTAINER}' is not running.")
        sys.exit(1)

    if len(sys.argv) > 1:
        topic = sys.argv[1]
        partitions = sys.argv[2] if len(sys.argv) > 2 else None
        replication = sys.argv[3] if len(sys.argv) > 3 else None
        ok = create_topic(topic, partitions, replication)
    else:
        ok = create_topic()
    sys.exit(0 if ok else 1)
