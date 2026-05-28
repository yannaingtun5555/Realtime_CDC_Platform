#!/usr/bin/env python3
"""
Interactive Kafka topic creator using docker exec (bypasses DNS issues).
Usage: python topic_creation.py
"""

import subprocess
import sys

KAFKA_CONTAINER = "cdc-kafka"
BOOTSTRAP_SERVER = "kafka:9092"  # internal address as seen by the container

def run_kafka_command(*args):
    """Run a kafka-topics command inside the Kafka container."""
    cmd = ["docker", "exec", KAFKA_CONTAINER, "kafka-topics", "--bootstrap-server", BOOTSTRAP_SERVER] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result

def create_topic():
    topic_name = input("Enter topic name (e.g., cdc.acme.db1.public.users): ").strip()
    if not topic_name:
        print("Topic name cannot be empty.")
        return

    try:
        partitions = int(input("Number of partitions [3]: ") or "3")
        replication = int(input("Replication factor [1]: ") or "1")
    except ValueError:
        print("Invalid number, using defaults.")
        partitions, replication = 3, 1

    # Check if topic already exists
    list_result = run_kafka_command("--list")
    existing_topics = set(list_result.stdout.strip().split())
    if topic_name in existing_topics:
        print(f"⚠️ Topic '{topic_name}' already exists.")
        return

    # Create topic
    create_result = run_kafka_command(
        "--create", "--topic", topic_name,
        "--partitions", str(partitions),
        "--replication-factor", str(replication)
    )

    if create_result.returncode == 0:
        print(f"✅ Topic '{topic_name}' created successfully.")
    else:
        print(f"❌ Failed to create topic: {create_result.stderr}")

if __name__ == "__main__":
    # Verify Kafka container is running
    check = subprocess.run(["docker", "ps", "--filter", f"name={KAFKA_CONTAINER}", "--format", "{{.Names}}"],
                           capture_output=True, text=True)
    if KAFKA_CONTAINER not in check.stdout:
        print(f"Error: Kafka container '{KAFKA_CONTAINER}' is not running.")
        sys.exit(1)

    create_topic()