#!/usr/bin/env python3
import json
import os
import time
import subprocess
import requests
import sys

# Environment variables
KAFKA_BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/opt/automation/config.json")
DB_CONFIG_PATH = os.getenv("DB_CONFIG_PATH", "/opt/automation/dbconfig.json")

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

def deploy_connector_for_topic(topic_name, db_config):
    # Extract database and table from topic name: cdc.<db>.?able
    parts = topic_name.split('.')
    if len(parts) != 3 or parts[0] != 'cdc':
        print(f"Skipping connector for topic {topic_name}: does not follow cdc.<db>.?able pattern")
        return
    database = parts[1]
    table = parts[2]

    connector_name = f"debezium-{database}-{table}"
    connector_config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": db_config["host"],
            "database.port": db_config["port"],
            "database.user": db_config["user"],
            "database.password": db_config["password"],
            "database.dbname": db_config["dbname"],
            "database.server.name": database,
            "table.include.list": f"{database}.{table}",
            "topic.prefix": "cdc",
            "plugin.name": "pgoutput",
            "publication.autocreate.mode": "filtered",
            "publication.name": f"dbz_publication_{database}_{table}",
            "slot.name": f"debezium_{database}_{table}",
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.unwrap.delete.handling.mode": "drop",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false"
        }
    }

    # Check if connector already exists
    resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
    if resp.status_code == 200:
        print(f"Connector {connector_name} already exists. Skipping creation.")
        return

    # Create connector
    print(f"Deploying connector {connector_name} for topic {topic_name}")
    resp = requests.post(f"{KAFKA_CONNECT_URL}/connectors", json=connector_config)
    if resp.status_code in (200, 201):
        print(f"Connector {connector_name} created successfully.")
    else:
        print(f"Failed to create connector {connector_name}: {resp.text}")
        sys.exit(1)

def main():
    print("Waiting for Kafka Connect to be ready...")
    for _ in range(30):
        try:
            resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors")
            if resp.status_code == 200:
                break
        except:
            pass
        time.sleep(2)
    else:
        print("Kafka Connect not ready after 60 seconds. Exiting.")
        sys.exit(1)

    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    with open(DB_CONFIG_PATH, 'r') as f:
        db_configs = json.load(f)
    # Build a lookup by database name
    db_lookup = {db["name"]: db for db in db_configs.get("databases", [])}

    topics = config.get("topics", [])
    for topic_info in topics:
        name = topic_info["name"]
        partitions = topic_info.get("partitions", 1)
        replication = topic_info.get("replication_factor", 1)
        create_topic(name, partitions, replication)
        if topic_info.get("type") == "external":
            # Extract database name from topic name
            parts = name.split('.')
            if len(parts) == 3 and parts[0] == 'cdc':
                db_name = parts[1]
                if db_name in db_lookup:
                    deploy_connector_for_topic(name, db_lookup[db_name])
                else:
                    print(f"No database config found for {db_name}, skipping connector for {name}")
            else:
                print(f"Topic {name} marked external but does not follow cdc.<db>.?able pattern. Skipping connector.")

    print("All topics and connectors processed.")

if __name__ == "__main__":
    main()