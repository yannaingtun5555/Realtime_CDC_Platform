#!/usr/bin/env python3
"""
Testing script for CDC pipeline: create two PostgreSQL databases with logical replication enabled.
"""

import docker
import time
import psycopg2
import tempfile
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATABASES = {
    "db1": {
        "name": "test_db1",
        "user": "cdc_user1",
        "password": "pass1234",
        "port": 54321,
        "container_name": "cdc_test_db1"
    },
    "db2": {
        "name": "test_db2",
        "user": "cdc_user2",
        "password": "pass5678",
        "port": 54322,
        "container_name": "cdc_test_db2"
    }
}

TABLES = {
    "users": """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW()
        );
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            amount DECIMAL(10,2),
            status VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        );
    """
}

INITIAL_DATA = [
    "INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com'), ('Bob', 'bob@test.com') ON CONFLICT DO NOTHING;",
    "INSERT INTO orders (user_id, amount, status) VALUES (1, 99.99, 'pending'), (2, 149.50, 'completed') ON CONFLICT DO NOTHING;"
]

def start_database(db_key):
    cfg = DATABASES[db_key]
    client = docker.from_env()
    
    # Remove existing container if any
    try:
        existing = client.containers.get(cfg["container_name"])
        existing.remove(force=True)
        logger.info(f"Removed existing container {cfg['container_name']}")
    except docker.errors.NotFound:
        pass
    
    logger.info(f"Starting PostgreSQL container '{cfg['container_name']}' on port {cfg['port']}")
    container = client.containers.run(
        "postgres:15",
        name=cfg["container_name"],
        environment={
            "POSTGRES_DB": cfg["name"],
            "POSTGRES_USER": cfg["user"],
            "POSTGRES_PASSWORD": cfg["password"]
        },
        ports={f"5432/tcp": cfg["port"]},
        command=[
            "postgres",
            "-c", "wal_level=logical",
            "-c", "max_replication_slots=10",
            "-c", "max_wal_senders=10"
        ],
        detach=True,
        remove=False
    )
    time.sleep(5)
    return container

def wait_for_db(host, port, user, password, dbname, retries=10):
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                dbname=dbname
            )
            conn.close()
            logger.info(f"Database {dbname} on port {port} is ready")
            return True
        except Exception as e:
            logger.info(f"Waiting for DB {dbname}... ({i+1}/{retries})")
            time.sleep(2)
    raise Exception(f"Database {dbname} did not become ready")

def setup_schema_and_data(host, port, user, password, dbname):
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname
    )
    conn.autocommit = True
    cur = conn.cursor()
    
    for table_name, ddl in TABLES.items():
        logger.info(f"Creating table {table_name} in {dbname}")
        cur.execute(ddl)
    
    for stmt in INITIAL_DATA:
        cur.execute(stmt)
    
    cur.close()
    conn.close()
    logger.info(f"Schema and data initialized for {dbname}")

def get_connection_string(db_key):
    cfg = DATABASES[db_key]
    return f"postgresql://{cfg['user']}:{cfg['password']}@localhost:{cfg['port']}/{cfg['name']}"

def generate_connector_config(db_key):
    cfg = DATABASES[db_key]
    return {
        "name": f"cdc-connector-{db_key}",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": "172.18.0.1",  # or host.docker.internal / gateway IP
            "database.port": str(cfg["port"]),
            "database.user": cfg["user"],
            "database.password": cfg["password"],
            "database.dbname": cfg["name"],
            "topic.prefix": f"cdc.test.{cfg['name']}",
            "plugin.name": "pgoutput",
            "slot.name": f"{db_key}_slot",
            "table.include.list": "public.users,public.orders",
            "snapshot.mode": "initial",
            "topic.creation.enable": "false"
        }
    }

def make_change(db_key):
    cfg = DATABASES[db_key]
    conn = psycopg2.connect(
        host="localhost",
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["name"]
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("INSERT INTO users (name, email) VALUES ('TestUser', 'test@example.com')")
    cur.close()
    conn.close()
    logger.info(f"Inserted test user into {db_key}")

def cleanup():
    client = docker.from_env()
    for key, cfg in DATABASES.items():
        try:
            container = client.containers.get(cfg["container_name"])
            logger.info(f"Stopping and removing {cfg['container_name']}")
            container.stop()
            container.remove()
        except docker.errors.NotFound:
            pass

def main():
    try:
        for db_key in DATABASES:
            start_database(db_key)
        
        for db_key, cfg in DATABASES.items():
            wait_for_db("localhost", cfg["port"], cfg["user"], cfg["password"], cfg["name"])
            setup_schema_and_data("localhost", cfg["port"], cfg["user"], cfg["password"], cfg["name"])
        
        print("\n=== CDC Test Databases Ready ===")
        for db_key in DATABASES:
            print(f"\n{db_key.upper()}:")
            print(f"  Connection string: {get_connection_string(db_key)}")
            import json
            print(f"  Connector config (use with host IP 172.18.0.1):")
            print(json.dumps(generate_connector_config(db_key), indent=2))
        
        make_change("db1")
        
        print("\n✅ Both databases are running with logical replication enabled. Press Ctrl+C to stop and clean up.")
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\nShutting down test databases...")
    finally:
        cleanup()
        print("Cleanup done.")

if __name__ == "__main__":
    main()