#!/usr/bin/env python3
"""
E2E: data-injector -> Debezium -> capture -> enrich -> internal.enriched
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from typing import Any

import requests
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")
FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
CONNECTOR_NAME = os.getenv("CONNECTOR_NAME", "debezium-inventory-connector")
REQUIRED_FLINK_JOBS = os.getenv(
    "REQUIRED_FLINK_JOBS",
    "CDC Capture Job,CDC Enrich Job,CDC Iceberg Sink Job",
).split(",")
CDC_TOPIC = os.getenv("CDC_TOPIC", "cdc.inventory.customers")
CAPTURE_TOPIC = os.getenv("CAPTURE_TOPIC", "internal.capture")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "internal.enriched")
WAIT_TIMEOUT_SEC = int(os.getenv("WAIT_TIMEOUT_SECONDS", "120"))
ENRICH_TIMEOUT_SEC = int(os.getenv("ENRICH_TIMEOUT_SECONDS", "90"))
POLL_MS = int(os.getenv("POLL_INTERVAL_MS", "500"))


def log(msg: str) -> None:
    print(msg, flush=True)


def wait_for_kafka(max_sec: int = 60) -> None:
    deadline = time.time() + max_sec
    while time.time() < deadline:
        try:
            consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, consumer_timeout_ms=3000)
            consumer.topics()
            consumer.close()
            log("Kafka is reachable.")
            return
        except NoBrokersAvailable:
            time.sleep(2)
    raise TimeoutError(f"Kafka not reachable at {KAFKA_BOOTSTRAP} within {max_sec}s")


def wait_for_connector(max_sec: int = 120) -> None:
    deadline = time.time() + max_sec
    url = f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status"
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code != 200:
                time.sleep(3)
                continue
            status = resp.json()
            conn_state = status.get("connector", {}).get("state")
            tasks = status.get("tasks", [])
            task_state = tasks[0].get("state") if tasks else None
            if conn_state == "RUNNING" and task_state == "RUNNING":
                log(f"Connector {CONNECTOR_NAME} is RUNNING.")
                return
        except requests.RequestException as exc:
            log(f"Connect poll error: {exc}")
        time.sleep(3)
    raise TimeoutError(f"Connector {CONNECTOR_NAME} not RUNNING within {max_sec}s")


def wait_for_flink_jobs(max_sec: int = 180) -> None:
    deadline = time.time() + max_sec
    required = [name.strip() for name in REQUIRED_FLINK_JOBS if name.strip()]
    while time.time() < deadline:
        try:
            resp = requests.get(f"{FLINK_REST_API}/jobs/overview", timeout=5)
            resp.raise_for_status()
            running = {
                job.get("name")
                for job in resp.json().get("jobs", [])
                if job.get("state") == "RUNNING"
            }
            missing = [name for name in required if name not in running]
            if not missing:
                log(f"All Flink jobs RUNNING: {required}")
                return
            log(f"Waiting for Flink jobs. Missing: {missing}")
        except requests.RequestException as exc:
            log(f"Flink poll error: {exc}")
        time.sleep(5)
    raise TimeoutError(f"Flink jobs not all RUNNING within {max_sec}s: {required}")


def _parse_json(raw: bytes | str) -> dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


def validate_debezium_event(event: dict[str, Any]) -> str:
    if "op" not in event or "source" not in event:
        raise AssertionError("CDC event missing op/source")
    source = event["source"]
    if source.get("table") != "customers":
        raise AssertionError(f"Unexpected table: {source.get('table')}")
    payload = event.get("after") or event.get("before")
    if not payload or not payload.get("email"):
        raise AssertionError("CDC payload missing email")
    return str(payload["email"])


def validate_capture_event(event: dict[str, Any]) -> str:
    for key in ("db_name", "table_name", "operation", "after", "ts_ms"):
        if key not in event:
            raise AssertionError(f"Capture event missing {key}")
    after = event.get("after") or {}
    email = after.get("email")
    if not email:
        raise AssertionError("Capture after missing email")
    return str(email)


def validate_enriched_event(event: dict[str, Any], expected_email: str) -> None:
    for key in ("db_name", "table_name", "operation", "op_type", "record_key", "payload"):
        if key not in event:
            raise AssertionError(f"Enriched event missing {key}")
    payload = json.loads(event["payload"])
    if payload.get("email") != expected_email:
        raise AssertionError(f"Enriched email mismatch: {payload.get('email')}")


def consume_topic(topic: str, validator, timeout_sec: int, group_prefix: str):
    group_id = f"{group_prefix}-{uuid.uuid4().hex[:8]}"
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=POLL_MS,
        value_deserializer=lambda m: m.decode("utf-8"),
    )
    deadline = time.time() + timeout_sec
    last_log = 0.0
    try:
        time.sleep(3)
        while time.time() < deadline:
            for _, batch in consumer.poll(timeout_ms=POLL_MS).items():
                for message in batch:
                    try:
                        event = _parse_json(message.value)
                        result = validator(event)
                        return event, result
                    except AssertionError as exc:
                        log(f"Skip message on {topic}: {exc}")
            now = time.time()
            if now - last_log >= 15:
                log(f"Waiting on {topic} (~{int(deadline - now)}s left)")
                last_log = now
    finally:
        consumer.close()
    raise TimeoutError(f"No valid message on {topic} within {timeout_sec}s")


def run_e2e() -> None:
    log("=== E2E: injector -> CDC -> capture -> enrich ===")
    wait_for_kafka()
    wait_for_connector()
    wait_for_flink_jobs()

    _, email = consume_topic(
        CDC_TOPIC,
        validate_debezium_event,
        WAIT_TIMEOUT_SEC,
        "e2e-cdc",
    )
    log(f"Debezium OK: {email}")

    _, capture_email = consume_topic(
        CAPTURE_TOPIC,
        validate_capture_event,
        ENRICH_TIMEOUT_SEC,
        "e2e-capture",
    )
    if capture_email != email:
        log(f"Note: latest capture email {capture_email} != debezium {email}")

    consume_topic(
        OUTPUT_TOPIC,
        lambda e: validate_enriched_event(e, email) or email,
        ENRICH_TIMEOUT_SEC,
        "e2e-enrich",
    )
    log(f"Enriched OK: {email}")
    log("=== E2E PASSED ===")


def main() -> None:
    try:
        run_e2e()
    except (TimeoutError, AssertionError) as exc:
        log(f"=== E2E FAILED: {exc} ===")
        sys.exit(1)
    except Exception as exc:
        log(f"=== E2E ERROR: {exc} ===")
        sys.exit(2)


if __name__ == "__main__":
    main()
