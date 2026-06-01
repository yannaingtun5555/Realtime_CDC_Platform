#!/usr/bin/env python3
"""
End-to-end test: wait for live rows from data-injector, verify Debezium CDC on Kafka,
then verify Flink-enriched events on internal.capture.
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from typing import Any, Optional

import requests
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")
FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
CONNECTOR_NAME = os.getenv("CONNECTOR_NAME", "debezium-inventory-connector")
FLINK_JOB_NAME = os.getenv("FLINK_JOB_NAME", "CDC Ingestion Job")
CDC_TOPIC = os.getenv("CDC_TOPIC", "cdc.inventory.customers")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "internal.capture")
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
            log(f"Connector state: connector={conn_state}, task={task_state}")
        except requests.RequestException as exc:
            log(f"Connect poll error: {exc}")
        time.sleep(3)
    raise TimeoutError(f"Connector {CONNECTOR_NAME} not RUNNING within {max_sec}s")


def wait_for_flink_job(max_sec: int = 120) -> None:
    deadline = time.time() + max_sec
    while time.time() < deadline:
        try:
            resp = requests.get(f"{FLINK_REST_API}/jobs/overview", timeout=5)
            resp.raise_for_status()
            for job in resp.json().get("jobs", []):
                if job.get("name") == FLINK_JOB_NAME and job.get("state") == "RUNNING":
                    log(f"Flink job {FLINK_JOB_NAME} is RUNNING ({job.get('jid')}).")
                    return
            log(f"Waiting for Flink job {FLINK_JOB_NAME}...")
        except requests.RequestException as exc:
            log(f"Flink poll error: {exc}")
        time.sleep(3)
    raise TimeoutError(f"Flink job {FLINK_JOB_NAME} not RUNNING within {max_sec}s")


def _parse_json(raw: bytes | str) -> dict[str, Any]:
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


def validate_debezium_event(event: dict[str, Any]) -> str:
    """Validate Debezium change event; return tracked email from after/before."""
    if "op" not in event:
        raise AssertionError("CDC event missing 'op'")
    if "source" not in event:
        raise AssertionError("CDC event missing 'source'")
    source = event["source"]
    if source.get("table") != "customers":
        raise AssertionError(f"Unexpected source.table: {source.get('table')}")
    op = event["op"]
    if op not in ("c", "r", "u"):
        raise AssertionError(f"Unexpected operation: {op}")
    payload = event.get("after") or event.get("before")
    if not payload:
        raise AssertionError("CDC event has no after/before payload")
    email = payload.get("email")
    if not email:
        raise AssertionError("CDC payload missing email (injector always sets email)")
    return str(email)


def validate_enriched_event(event: dict[str, Any], expected_email: str) -> None:
    required = ("db_name", "table_name", "operation", "after", "ts_ms")
    for key in required:
        if key not in event:
            raise AssertionError(f"Enriched event missing '{key}'")
    if event["table_name"] != "customers":
        raise AssertionError(f"Unexpected table_name: {event['table_name']}")
    after = event["after"]
    if not isinstance(after, dict):
        raise AssertionError("Enriched 'after' is not an object")
    if after.get("email") != expected_email:
        raise AssertionError(
            f"Email mismatch: expected {expected_email}, got {after.get('email')}"
        )


def consume_live_cdc(timeout_sec: int) -> tuple[dict[str, Any], str]:
    """Consume the next live CDC event from data-injector (auto_offset_reset=latest)."""
    group_id = f"e2e-cdc-{uuid.uuid4().hex[:8]}"
    consumer = KafkaConsumer(
        CDC_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=POLL_MS,
        value_deserializer=lambda m: m.decode("utf-8"),
    )
    log(f"Listening on {CDC_TOPIC} for live injector events (timeout {timeout_sec}s)...")
    deadline = time.time() + timeout_sec
    last_progress_log = 0.0
    try:
        # Allow partition assignment before expecting new injector rows
        time.sleep(3)
        while time.time() < deadline:
            records = consumer.poll(timeout_ms=POLL_MS)
            for tp, batch in records.items():
                for message in batch:
                    try:
                        event = _parse_json(message.value)
                        email = validate_debezium_event(event)
                        log(f"CDC event OK: op={event.get('op')} email={email}")
                        return event, email
                    except AssertionError as exc:
                        log(f"Skipping invalid CDC message: {exc}")
            now = time.time()
            if now - last_progress_log >= 15:
                log(f"Still waiting for CDC event... ~{int(deadline - now)}s left")
                last_progress_log = now
    finally:
        consumer.close()
    raise TimeoutError(
        f"No valid live CDC event on {CDC_TOPIC} within {timeout_sec}s. "
        "Is data-injector running and inserting into inventory.customers?"
    )


def consume_matching_enriched(expected_email: str, timeout_sec: int) -> dict[str, Any]:
    group_id = f"e2e-enrich-{uuid.uuid4().hex[:8]}"
    consumer = KafkaConsumer(
        OUTPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=POLL_MS,
        value_deserializer=lambda m: m.decode("utf-8"),
    )
    log(f"Listening on {OUTPUT_TOPIC} for enriched email={expected_email}...")
    deadline = time.time() + timeout_sec
    try:
        time.sleep(2)
        while time.time() < deadline:
            records = consumer.poll(timeout_ms=POLL_MS)
            for tp, batch in records.items():
                for message in batch:
                    try:
                        event = _parse_json(message.value)
                        validate_enriched_event(event, expected_email)
                        log(f"Enriched event OK: operation={event.get('operation')}")
                        return event
                    except AssertionError:
                        continue
    finally:
        consumer.close()
    raise TimeoutError(
        f"No enriched event for {expected_email} on {OUTPUT_TOPIC} within {timeout_sec}s. "
        "Check Flink job CDC Ingestion Job."
    )


def run_e2e() -> None:
    log("=== streamlakeCDC E2E: live injector → Debezium → Flink ===")
    wait_for_kafka()
    wait_for_connector()
    wait_for_flink_job()

    _cdc_event, email = consume_live_cdc(WAIT_TIMEOUT_SEC)
    consume_matching_enriched(email, ENRICH_TIMEOUT_SEC)

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
