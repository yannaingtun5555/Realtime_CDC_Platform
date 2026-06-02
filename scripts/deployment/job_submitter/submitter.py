#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys
import time

import requests
from kafka import KafkaConsumer

FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/opt/job-submitter/config.json")
SAVEPOINT_BASE_DIR = os.getenv("SAVEPOINT_BASE_DIR", "s3a://flink-savepoints")
MINIO_ALIAS = "local"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

JOB_ID_PATTERN = re.compile(r"JobID\s+([a-f0-9]+)", re.IGNORECASE)


def topic_exists(topic_name: str) -> bool:
    consumer = None
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id="topic-checker",
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000,
        )
        return topic_name in consumer.topics()
    except Exception as exc:
        print(f"Error checking topic {topic_name}: {exc}")
        return False
    finally:
        if consumer:
            consumer.close()


def wait_for_topics(topics: list[str], max_attempts: int = 30, delay: int = 2) -> None:
    for topic in topics:
        for attempt in range(max_attempts):
            if topic_exists(topic):
                print(f"Topic {topic} exists.")
                break
            print(f"Waiting for topic {topic}... ({attempt + 1}/{max_attempts})")
            time.sleep(delay)
        else:
            print(f"Topic {topic} still missing after {max_attempts * delay}s.")
            sys.exit(1)


def setup_mc_alias() -> None:
    subprocess.run(
        ["mc", "alias", "set", MINIO_ALIAS, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY],
        capture_output=True,
    )


def get_latest_savepoint(job_name: str) -> str | None:
    dir_path = f"{MINIO_ALIAS}/{SAVEPOINT_BASE_DIR.replace('s3a://', '')}/{job_name.replace(' ', '_')}/"
    result = subprocess.run(["mc", "ls", "--json", dir_path], capture_output=True, text=True)
    if result.returncode != 0:
        return None
    savepoints = []
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        try:
            entry = json.loads(line)
            if entry.get("type") == "folder":
                savepoints.append((entry.get("lastModified"), entry.get("key")))
        except json.JSONDecodeError:
            continue
    if not savepoints:
        return None
    savepoints.sort(reverse=True)
    latest_key = savepoints[0][1]
    # BUG FIX: mc ls --json returns folder keys with a trailing '/'; strip it
    # so the assembled savepoint path doesn't contain a double slash.
    return f"{SAVEPOINT_BASE_DIR}/{job_name.replace(' ', '_')}/{latest_key.rstrip('/')}"


def job_is_running(job_name: str) -> bool:
    try:
        resp = requests.get(f"{FLINK_REST_API}/jobs/overview", timeout=5)
        resp.raise_for_status()
        for job in resp.json().get("jobs", []):
            if job.get("name") == job_name and job.get("state") == "RUNNING":
                return True
    except requests.RequestException as exc:
        print(f"Error checking Flink jobs: {exc}")
    return False


def job_id_is_running(job_id: str) -> bool:
    try:
        resp = requests.get(f"{FLINK_REST_API}/jobs/{job_id}", timeout=5)
        resp.raise_for_status()
        state = resp.json().get("state")
        return state in ("RUNNING", "CREATED", "RESTARTING")
    except requests.RequestException:
        return False


def extract_job_id(output: str) -> str | None:
    match = JOB_ID_PATTERN.search(output)
    return match.group(1) if match else None


def submit_job(job_config: dict, from_savepoint: str | None = None) -> tuple[bool, str | None]:
    print(f"Submitting job: {job_config['name']}")
    jobmanager = os.getenv("FLINK_JOBMANAGER", "flink-jobmanager:8081")
    cmd = ["flink", "run", "-m", jobmanager, "-d"]
    if from_savepoint:
        cmd.extend(["--fromSavepoint", from_savepoint])

    # BUG FIX: Table API jobs (table_api: true) manage their own JARs via
    # pipeline.jars inside the Python script. Passing the same Iceberg/Hadoop
    # JARs via '-C' loads them into the Flink CLI JVM, which causes a
    # LinkageError (commons-cli version conflict with hadoop-common).
    # The Dockerfile comment explicitly warns: keep these out of /opt/flink/lib
    # — the same logic applies to '-C' which also loads into the CLI JVM.
    # For DataStream API jobs (01, 02), '-C' is the correct mechanism.
    if not job_config.get("table_api"):
        for jar in job_config.get("jars", []):
            cmd.extend(["-C", jar])

    cmd.extend(["-py", job_config["py_file"]])
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    combined = (result.stdout or "") + (result.stderr or "")
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())
    job_id = extract_job_id(combined)
    if job_id:
        print(f"Flink returned JobID: {job_id}")
    return result.returncode == 0, job_id


def verify_job_running(
    job_name: str,
    job_id: str | None = None,
    max_attempts: int = 10,
    delay: int = 5,
) -> bool:
    for attempt in range(max_attempts):
        if job_is_running(job_name):
            print(f"Job {job_name} is RUNNING (by name).")
            return True
        if job_id and job_id_is_running(job_id):
            print(f"Job {job_name} is active (JobID {job_id}).")
            return True
        print(f"Waiting for {job_name}... ({attempt + 1}/{max_attempts})")
        time.sleep(delay)
    return False


def main() -> None:
    print("Job submitter started.")
    setup_mc_alias()

    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        config = json.load(handle)

    jobs = config.get("jobs", [])
    if not jobs:
        print("No jobs defined in config.")
        return

    all_topics: set[str] = set()
    for job in jobs:
        all_topics.update(job.get("input_topics", []))
        if "output_topic" in job:
            all_topics.add(job["output_topic"])
    for topic_info in config.get("topics", []):
        all_topics.add(topic_info["name"])
    if all_topics:
        print("Ensuring required topics exist...")
        wait_for_topics(sorted(all_topics))

    for job in jobs:
        job_name = job["name"]
        if job_is_running(job_name):
            print(f"Job {job_name} already running. Skipping.")
            continue

        savepoint = get_latest_savepoint(job_name)
        if savepoint:
            print(f"Restoring {job_name} from {savepoint}")
        else:
            print(f"No savepoint for {job_name}; starting fresh.")

        success, job_id = submit_job(job, from_savepoint=savepoint)
        if not success:
            print(f"ERROR: flink run failed for {job_name}.")
            sys.exit(1)

        verify_attempts = 10
        verify_delay = 5
        if job.get("table_api"):
            verify_attempts = max(10, job.get("verify_timeout_seconds", 90) // verify_delay)

        if verify_job_running(job_name, job_id, verify_attempts, verify_delay):
            print(f"Job {job_name} submitted successfully.")
        else:
            print(f"ERROR: {job_name} not confirmed RUNNING after submit.")
            sys.exit(1)

    print("All jobs submitted.")
    sys.exit(0)


if __name__ == "__main__":
    main()
