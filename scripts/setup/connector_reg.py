#!/usr/bin/env python3
"""
Reads .env, loops over all defined databases, and registers a Debezium connector
for each. Assumes all required topics have been created manually beforehand.
"""

import os
import re
import requests
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv:
    load_dotenv()

KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083")

def register_postgres_connector(connector_name, db_host, db_port, db_user, db_password,
                                db_name, topic_prefix, tables, slot_name, snapshot_mode="initial"):
    config = {
        "name": connector_name,
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": db_host,
            "database.port": str(db_port),
            "database.user": db_user,
            "database.password": db_password,
            "database.dbname": db_name,
            "topic.prefix": topic_prefix,
            "plugin.name": "pgoutput",
            "slot.name": slot_name,
            "table.include.list": tables,
            "tombstones.on.delete": "false",
            "include.schema.changes": "false",
            "snapshot.mode": snapshot_mode,
            "topic.creation.enable": "false",          # no auto creation
            "topic.creation.default.replication.factor": "1",   # REQUIRED even when creation disabled
            "topic.creation.default.partitions": "3",           # REQUIRED even when creation disabled
            "errors.tolerance": "none",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false"
        }
    }

    url = f"{KAFKA_CONNECT_URL}/connectors"
    resp = requests.post(url, json=config, headers={"Content-Type": "application/json"})
    if resp.status_code in (200, 201, 202):
        print(f"Registered {connector_name}")
        return True
    if resp.status_code == 409:
        update_url = f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/config"
        update_resp = requests.put(update_url, json=config["config"], headers={"Content-Type": "application/json"})
        if update_resp.status_code in (200, 201, 202):
            print(f"Updated existing connector {connector_name}")
            return True
        print(f"Failed to update {connector_name}: {update_resp.status_code} - {update_resp.text}")
        return False
    else:
        print(f"Failed {connector_name}: {resp.status_code} - {resp.text}")
        return False

def discover_databases():
    """Find all DB configurations in .env by looking for DB<n>_HOST variables"""
    db_configs = {}
    pattern = re.compile(r'^DB(\d+)_HOST$')
    for key in os.environ:
        m = pattern.match(key)
        if m:
            idx = m.group(1)
            # gather all fields for this index
            config = {
                "name": os.getenv(f"DB{idx}_NAME"),
                "tenant": os.getenv(f"DB{idx}_TENANT", "default"),
                "host": os.getenv(f"DB{idx}_HOST"),
                "port": os.getenv(f"DB{idx}_PORT", "5432"),
                "user": os.getenv(f"DB{idx}_USER"),
                "password": os.getenv(f"DB{idx}_PASSWORD"),
                "dbname": os.getenv(f"DB{idx}_DBNAME"),
                "tables": os.getenv(f"DB{idx}_TABLES"),
                "slot": os.getenv(f"DB{idx}_SLOT", f"cdc_slot_{idx}")
            }
            # validate required fields
            if all(config.values()):
                db_configs[idx] = config
            else:
                missing = [k for k, v in config.items() if v is None]
                print(f"Skipping DB{idx} - missing: {missing}")
    return db_configs

def main():
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=5)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"Kafka Connect is not reachable at {KAFKA_CONNECT_URL}: {exc}")
        return False

    db_configs = discover_databases()
    if not db_configs:
        print("No database configurations found in .env (need DB1_HOST, DB1_NAME, ...).")
        return False

    ok = True
    for idx, cfg in db_configs.items():
        topic_prefix = f"cdc.{cfg['tenant']}.{cfg['name']}"
        connector_name = f"cdc-connector-{cfg['tenant']}-{cfg['name']}"

        print(f"\nRegistering connector for DB{idx}: {cfg['name']}")
        print(f"   Topics prefix: {topic_prefix}")
        print(f"   Tables: {cfg['tables']}")
        print(f"   Slot: {cfg['slot']}")

        success = register_postgres_connector(
            connector_name=connector_name,
            db_host=cfg['host'],
            db_port=int(cfg['port']),
            db_user=cfg['user'],
            db_password=cfg['password'],
            db_name=cfg['dbname'],
            topic_prefix=topic_prefix,
            tables=cfg['tables'],
            slot_name=cfg['slot']
        )
        if not success:
            print(f"   Make sure the topics ({topic_prefix}.*) exist. Run topic_creation.py first.")
            ok = False
    return ok

if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
