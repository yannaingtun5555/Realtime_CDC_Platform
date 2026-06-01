#!/usr/bin/env python3
import json
import os
import time
import requests
import sys

try:
    import psycopg2
except ImportError:
    psycopg2 = None

KAFKA_CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")
DB_CONFIG_PATH = os.getenv("DB_CONFIG_PATH", "/opt/automation/dbconfig.json")
CONNECTORS_DIR = os.getenv("CONNECTORS_DIR", "/opt/automation/connectors")


def delete_connector(name: str) -> None:
    try:
        resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{name}", timeout=5)
        if resp.status_code == 200:
            print(f"Deleting existing connector {name} ...")
            requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{name}", timeout=10)
            time.sleep(2)
    except requests.RequestException:
        pass


def wait_for_connector(connector_name: str, timeout: int = 120) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(
                f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status",
                timeout=5,
            )
            if resp.status_code != 200:
                time.sleep(3)
                continue
            status = resp.json()
            conn_state = status.get("connector", {}).get("state")
            tasks = status.get("tasks", [])
            task_state = tasks[0].get("state") if tasks else None
            if conn_state == "RUNNING" and task_state == "RUNNING":
                print(f"Connector {connector_name} is RUNNING.")
                return True
        except requests.RequestException:
            pass
        print(f"Waiting for connector {connector_name}...")
        time.sleep(3)
    return False


def create_connector(connector_name: str, config: dict) -> None:
    payload = {"name": connector_name, "config": config}
    resp = requests.post(f"{KAFKA_CONNECT_URL}/connectors", json=payload, timeout=30)
    if resp.status_code in (200, 201):
        print(f"Connector {connector_name} created successfully.")
        return
    print(f"Failed to create connector {connector_name}: {resp.text}")
    sys.exit(1)


def prepare_postgres_tables(db_info: dict) -> None:
    if psycopg2 is None:
        print("psycopg2 not installed; skipping DB preparation.")
        return
    schema = db_info.get("schema", db_info["name"])
    tables = db_info.get("tables", [])
    if not tables:
        return
    try:
        conn = psycopg2.connect(
            host=db_info["host"],
            port=db_info.get("port", 5432),
            user=db_info["user"],
            password=db_info["password"],
            database=db_info["dbname"],
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for table in tables:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255)
                )
                """
            )
        cur.close()
        conn.close()
        print(f"Prepared schema {schema} tables: {tables}")
    except Exception as exc:
        print(f"DB preparation failed: {exc}")
        sys.exit(1)


def build_connector_config(db_info: dict, connector_def: dict) -> dict:
    db_type = db_info.get("type", "postgres")
    base = {
        "connector.class": {
            "postgres": "io.debezium.connector.postgresql.PostgresConnector",
            "mysql": "io.debezium.connector.mysql.MySqlConnector",
        }.get(db_type, "io.debezium.connector.postgresql.PostgresConnector"),
        "database.hostname": db_info["host"],
        "database.port": str(db_info.get("port", 5432)),
        "database.user": db_info["user"],
        "database.password": db_info["password"],
        "database.dbname": db_info["dbname"],
        "database.server.name": connector_def.get("database_server_name", db_info["name"]),
        "table.include.list": connector_def["table_include_list"],
        "topic.prefix": connector_def.get("topic_prefix", "cdc"),
        "snapshot.mode": connector_def.get("snapshot_mode", "initial"),
        "heartbeat.interval.ms": str(connector_def.get("heartbeat_interval_ms", 5000)),
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
    }
    if db_type == "postgres":
        base["plugin.name"] = connector_def.get("plugin_name", "pgoutput")
        base["publication.autocreate.mode"] = connector_def.get(
            "publication_autocreate_mode", "filtered"
        )
        base["publication.name"] = connector_def.get(
            "publication_name", f"dbz_pub_{db_info['name']}"
        )
        base["slot.name"] = connector_def.get("slot_name", f"dbz_slot_{db_info['name']}")
    return base


def main() -> None:
    print("Waiting for Kafka Connect...")
    for _ in range(60):
        try:
            resp = requests.get(f"{KAFKA_CONNECT_URL}/connectors", timeout=5)
            if resp.status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(2)
    else:
        print("Kafka Connect not ready.")
        sys.exit(1)

    with open(DB_CONFIG_PATH, "r", encoding="utf-8") as handle:
        db_configs = json.load(handle)
    db_lookup = {db["name"]: db for db in db_configs.get("databases", [])}

    if not os.path.isdir(CONNECTORS_DIR):
        print(f"Connectors directory {CONNECTORS_DIR} not found.")
        sys.exit(1)

    for file in sorted(os.listdir(CONNECTORS_DIR)):
        if not file.endswith(".json"):
            continue
        path = os.path.join(CONNECTORS_DIR, file)
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        db_name = data.get("database_name")
        if db_name not in db_lookup:
            print(f"Database {db_name} missing from dbconfig.json, skipping {file}")
            continue
        db_info = db_lookup[db_name]
        if db_info.get("type", "postgres") == "postgres":
            prepare_postgres_tables(db_info)
        for connector_def in data.get("connectors", []):
            connector_name = connector_def["name"]
            delete_connector(connector_name)
            config = build_connector_config(db_info, connector_def)
            create_connector(connector_name, config)
            if not wait_for_connector(connector_name):
                print(f"Connector {connector_name} failed to reach RUNNING.")
                sys.exit(1)

    print("All connectors registered.")


if __name__ == "__main__":
    main()
