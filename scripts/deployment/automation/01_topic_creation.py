#!/usr/bin/env python3
import json
import os
import subprocess
import sys

KAFKA_BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/opt/automation/config.json")

def create_topic(name, partitions, replication_factor=1):
    print(f"Creating topic {name} with partitions={partitions}, rf={replication_factor}")
    cmd = [
        "kafka-topics.sh",
        "--create",
        "--if-not-exists",
        "--bootstrap-server", KAFKA_BOOTSTRAP,
        "--topic", name,
        "--partitions", str(partitions),
        "--replication-factor", str(replication_factor)
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"Topic {name} created or already exists.")
    else:
        print(f"Failed to create topic {name}: {result.stderr}")
        sys.exit(1)

def main():
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)

    topics = config.get("topics", [])
    if not topics:
        print("No topics defined. Exiting.")
        return

    for topic in topics:
        name = topic["name"]
        partitions = topic.get("partitions", 1)
        replication = topic.get("replication_factor", 1)
        create_topic(name, partitions, replication)

    print("All topics created.")

if __name__ == "__main__":
    main()