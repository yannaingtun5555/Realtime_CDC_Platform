#!/usr/bin/env python3
import json
import os
import subprocess
import requests
import time
import sys
from kafka.admin import KafkaAdminClient
from kafka import KafkaConsumer

# ========== ENVIRONMENT ==========
FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/opt/job-submitter/config.json")
SAVEPOINT_BASE_DIR = os.getenv("SAVEPOINT_BASE_DIR", "s3a://flink-savepoints")
MINIO_ALIAS = "local"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

def get_kafka_admin():
    return KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP, client_id='job-submitter')

def topic_exists(topic_name):
    consumer = None
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            client_id='topic-checker',
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=5000
        )
        topics = consumer.topics()
        return topic_name in topics
    except Exception as e:
        print(f"Error checking topic {topic_name}: {e}")
        return False
    finally:
        if consumer:
            consumer.close()

def wait_for_topics(topics, max_attempts=30, delay=2):
    for topic in topics:
        for attempt in range(max_attempts):
            if topic_exists(topic):
                print(f"Topic {topic} exists.")
                break
            print(f"Waiting for topic {topic}... ({attempt+1}/{max_attempts})")
            time.sleep(delay)
        else:
            print(f"Topic {topic} still missing after {max_attempts*delay}s. Exiting.")
            sys.exit(1)

def setup_mc_alias():
    subprocess.run([
        "mc", "alias", "set", MINIO_ALIAS, MINIO_ENDPOINT,
        MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    ], capture_output=True)

def get_latest_savepoint(job_name):
    dir_path = f"{MINIO_ALIAS}/{SAVEPOINT_BASE_DIR.replace('s3a://', '')}/{job_name.replace(' ', '_')}/"
    cmd = ["mc", "ls", "--json", dir_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    savepoints = []
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        try:
            entry = json.loads(line)
            if entry.get('type') == 'folder':
                savepoints.append((entry.get('lastModified'), entry.get('key')))
        except:
            pass
    if not savepoints:
        return None
    savepoints.sort(reverse=True)
    latest_key = savepoints[0][1]
    full_path = f"{SAVEPOINT_BASE_DIR}/{job_name.replace(' ', '_')}/{latest_key}"
    print(f"Latest savepoint for {job_name}: {full_path}")
    return full_path

def job_is_running(job_name):
    try:
        resp = requests.get(f"{FLINK_REST_API}/jobs/overview", timeout=5)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        for job in jobs:
            if job.get("name") == job_name and job.get("state") == "RUNNING":
                return True
    except Exception as e:
        print(f"Error checking Flink jobs: {e}")
    return False

def submit_job(job_config, from_savepoint=None):
    print(f"Submitting job: {job_config['name']}")
    jobmanager = os.getenv("FLINK_JOBMANAGER", "flink-jobmanager:8081")
    cmd = ["flink", "run", "-m", jobmanager, "-d"]
    if from_savepoint:
        cmd.extend(["--fromSavepoint", from_savepoint])
    for jar in job_config.get("jars", []):
        cmd.extend(["-C", jar])
    # Only pass the Python script – no additional arguments
    cmd.extend(["-py", job_config["py_file"]])
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())
    return result.returncode == 0

def verify_job_running(job_name, max_attempts=10, delay=5):
    for attempt in range(max_attempts):
        if job_is_running(job_name):
            print(f"Job {job_name} is now RUNNING.")
            return True
        print(f"Waiting for job {job_name} to start... ({attempt+1}/{max_attempts})")
        time.sleep(delay)
    print(f"Job {job_name} did not become RUNNING after {max_attempts*delay}s.")
    return False

def main():
    print("Job submitter started.")
    setup_mc_alias()

    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)

    jobs = config.get("jobs", [])
    if not jobs:
        print("No jobs defined in config. Exiting.")
        return

    # Wait for input/output topics before submitting (avoids race with automation)
    all_topics = set()
    for job in jobs:
        for topic in job.get("input_topics", []):
            all_topics.add(topic)
        if "output_topic" in job:
            all_topics.add(job["output_topic"])
    for topic_info in config.get("topics", []):
        if topic_info.get("type") == "external":
            all_topics.add(topic_info["name"])
    if all_topics:
        print("Ensuring all required topics exist...")
        wait_for_topics(list(all_topics))

    for job in jobs:
        job_name = job["name"]
        if job_is_running(job_name):
            print(f"Job {job_name} is already running. Skipping submission.")
            continue

        print(f"Job {job_name} not running. Preparing submission...")
        savepoint = get_latest_savepoint(job_name)
        if savepoint:
            print(f"Will restore from savepoint: {savepoint}")
        else:
            print("No savepoint found; starting fresh.")

        success = submit_job(job, from_savepoint=savepoint)
        if success:
            if verify_job_running(job_name):
                print(f"Job {job_name} submitted and running successfully.")
            else:
                print(f"WARNING: Job {job_name} submitted but not confirmed RUNNING.")
        else:
            print(f"ERROR: Failed to submit job {job_name}.")
            sys.exit(1)

    print("All jobs processed. Exiting.")
    sys.exit(0)

if __name__ == "__main__":
    main()