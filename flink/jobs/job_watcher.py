#!/usr/bin/env python3
import os
import requests
import subprocess
import time

FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
JOB_FILE = os.getenv("JOB_FILE", "/opt/flink/jobs/01_capture.py")
EXPECTED_JOB_NAME = os.getenv("EXPECTED_JOB_NAME", "CDC Ingestion Job")
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "30"))
JARS = [
    jar.strip()
    for jar in os.getenv(
        "FLINK_CLASSPATH_JARS",
        "file:///opt/flink/jars/flink-connector-kafka-3.2.0-1.18.jar,"
        "file:///opt/flink/jars/kafka-clients-3.4.0.jar",
    ).split(",")
    if jar.strip()
]


def get_job_status():
    try:
        resp = requests.get(f"{FLINK_REST_API}/jobs/overview", timeout=5)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        matching = [job for job in jobs if job.get("name") == EXPECTED_JOB_NAME]
        active = [
            job for job in matching
            if job.get("state") in {"CREATED", "RUNNING", "RESTARTING", "RECONCILING"}
        ]
        failed = [
            job for job in matching
            if job.get("state") in {"FAILED", "CANCELED", "CANCELLING", "SUSPENDED"}
        ]
        return active, failed
    except Exception as e:
        print(f"Error checking jobs: {e}")
        return [], []


def submit_job():
    print("Submitting ingestion job...")
    cmd = ["flink", "run", "-d"]
    for jar in JARS:
        cmd.extend(["-C", jar])
    cmd.extend(["-py", JOB_FILE])
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())
    return result.returncode == 0


def main():
    print("Job watcher started.")
    while True:
        active_jobs, failed_jobs = get_job_status()
        if active_jobs:
            job_ids = ", ".join(job.get("jid", "unknown") for job in active_jobs)
            print(f"{EXPECTED_JOB_NAME} is active: {job_ids}")
        else:
            if failed_jobs:
                states = ", ".join(f"{job.get('jid')}={job.get('state')}" for job in failed_jobs)
                print(f"Found failed/stopped {EXPECTED_JOB_NAME} jobs: {states}. Resubmitting...")
            else:
                print(f"No active {EXPECTED_JOB_NAME} found. Submitting...")
            submit_job()
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
