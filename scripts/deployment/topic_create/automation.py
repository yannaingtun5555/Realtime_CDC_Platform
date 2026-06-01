#!/usr/bin/env python3
import json
import os
import time
import subprocess
import requests
import sys
import psycopg2
from collections import defaultdict

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

def delete_connector(connector_name):
    """Delete connector if it exists."""
    resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
    if resp.status_code == 200:
        print(f"Deleting existing connector {connector_name} ...")
        requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
        time.sleep(2)

def wait_for_connector(connector_name, timeout=120):
    """Wait for the connector and its task to become RUNNING."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status")
            if resp.status_code != 200:
                time.sleep(5)
                continue
            status = resp.json()
            connector_state = status.get("connector", {}).get("state")
            tasks = status.get("tasks", [])
            task_state = tasks[0].get("state") if tasks else None
            if connector_state == "RUNNING" and task_state == "RUNNING":
                print(f"Connector {connector_name} is RUNNING.")
                time.sleep(5)
                return True
        except Exception:
            pass
        print(f"Waiting for connector {connector_name} to be RUNNING...")
        time.sleep(5)
    print(f"Connector {connector_name} did not become RUNNING within {timeout}s.")
    return False

def deploy_connector_for_database(db_name, db_config, tables):
    """Create a single Debezium connector for all tables in this database."""
    connector_name = f"debezium-{db_name}-connector"
    table_list = ",".join([f"{db_name}.{t}" for t in tables])
    connector_config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": db_config["host"],
            "database.port": str(db_config.get("port", 5432)),
            "database.user": db_config["user"],
            "database.password": db_config["password"],
            "database.dbname": db_config["dbname"],
            "database.server.name": db_name,
            "table.include.list": table_list,
            "plugin.name": "pgoutput",
            "publication.autocreate.mode": "filtered",
            "publication.name": f"dbz_pub_{db_name}",
            "slot.name": f"dbz_slot_{db_name}",
            "snapshot.mode": "initial",
            "heartbeat.interval.ms": "5000",
            "topic.prefix": "cdc",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false"
        }
    }
    delete_connector(connector_name)

    print(f"Deploying connector {connector_name} for tables: {table_list}")
    resp = requests.post(f"{KAFKA_CONNECT_URL}/connectors", json=connector_config)
    if resp.status_code not in (200, 201):
        print(f"Failed to create connector {connector_name}: {resp.text}")
        sys.exit(1)
    print(f"Connector {connector_name} created successfully.")

    if not wait_for_connector(connector_name):
        print(f"Connector {connector_name} failed to start. Exiting.")
        sys.exit(1)

def ensure_table_exists(db_name, db_config, tables):
    """Create schema and tables if they don't exist, and insert seed data using psycopg2."""
    try:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config.get("port", 5432),
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["dbname"]
        )
        conn.autocommit = True
        cur = conn.cursor()
        for table in tables:
            cur.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {db_name};
                CREATE TABLE IF NOT EXISTS {db_name}.{table} (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255)
                );
            """)
            cur.execute(f"SELECT 1 FROM {db_name}.{table} LIMIT 1;")
            if cur.fetchone() is None:
                cur.execute(f"""
                    INSERT INTO {db_name}.{table} (first_name, last_name, email) VALUES
                        ('John', 'Doe', 'john@example.com'),
                        ('Jane', 'Smith', 'jane@example.com');
                """)
        cur.close()
        conn.close()
        print(f"Tables {tables} are ready with seed data.")
    except Exception as e:
        print(f"Failed to prepare tables: {e}")
        sys.exit(1)

def main():
    print("Waiting for Kafka Connect to be ready...")
    for _ in range(60):
        try:
            resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors")
            if resp.status_code == 200:
                break
        except:
            pass
        time.sleep(2)
    else:
        print("Kafka Connect not ready after 120 seconds. Exiting.")
        sys.exit(1)

    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    with open(DB_CONFIG_PATH, 'r') as f:
        db_configs = json.load(f)

    db_lookup = {db["name"]: db for db in db_configs.get("databases", [])}

    topics_by_db = defaultdict(list)
    topics = config.get("topics", [])
    for topic_info in topics:
        name = topic_info["name"]
        partitions = topic_info.get("partitions", 1)
        replication = topic_info.get("replication_factor", 1)
        create_topic(name, partitions, replication)
        if topic_info.get("type") == "external":
            parts = name.split('.')
            if len(parts) == 3 and parts[0] == 'cdc':
                db_name = parts[1]
                table_name = parts[2]
                topics_by_db[db_name].append(table_name)
            else:
                print(f"Topic {name} marked external but doesn't follow cdc.<db>.?able pattern. Skipping connector.")

    for db_name, tables in topics_by_db.items():
        if db_name not in db_lookup:
            print(f"No database config found for {db_name}, skipping connector.")
            continue
        db_config = db_lookup[db_name]
        ensure_table_exists(db_name, db_config, tables)
        deploy_connector_for_database(db_name, db_config, tables)

    print("All topics and connectors processed.")
    sys.exit(0)

if __name__ == "__main__":
    main()