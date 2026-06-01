#!/usr/bin/env python3
import time
import random
import psycopg2
import os
from datetime import datetime

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_NAME = os.getenv("DB_NAME", "inventory")
INTERVAL = int(os.getenv("INJECT_INTERVAL_SECONDS", "10"))

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def main():
    print(f"Starting data injector. Inserting a random customer every {INTERVAL} seconds.")
    while True:
        conn = None
        try:
            conn = get_connection()
            cur = conn.cursor()
            # Ensure table exists
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS inventory;
                CREATE TABLE IF NOT EXISTS inventory.customers (
                    id SERIAL PRIMARY KEY,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    email VARCHAR(255),
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            # Insert random data
            first_names = ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Frank', 'Grace', 'Henry', 'Ivy', 'Jack']
            last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
            first = random.choice(first_names)
            last = random.choice(last_names)
            email = f"{first.lower()}.{last.lower()}{random.randint(1,1000)}@example.com"
            cur.execute(
                "INSERT INTO inventory.customers (first_name, last_name, email) VALUES (%s, %s, %s)",
                (first, last, email)
            )
            conn.commit()
            cur.close()
            print(f"[{datetime.now().isoformat()}] Inserted: {first} {last} <{email}>")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            # BUG FIX: conn was only closed on the happy path. Any exception
            # between get_connection() and conn.close() left the connection
            # open, slowly exhausting Postgres's max_connections.
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()