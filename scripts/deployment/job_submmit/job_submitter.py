#!/usr/bin/env python3
"""
Submit three Flink jobs (ingestion, enrichment, sink) to the Flink cluster.
Uses docker exec to run 'flink run' inside the JobManager container.
"""

import subprocess
import time
import sys
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
FLINK_JOBMANAGER_CONTAINER = "cdc-flink-jobmanager"
FLINK_REST_API = "http://flink-jobmanager:8081"
JOBS_DIR = "/opt/flink/jobs"
JOBS = [
    ("01_ingestion.py", "Ingestion Job"),
    ("02_enrichment.py", "Enrichment Job"),
    ("03_sink.py", "Sink to Iceberg Job")
]

def run_docker_exec(command):
    """Run a command inside the Flink JobManager container."""
    cmd = ["docker", "exec", FLINK_JOBMANAGER_CONTAINER] + command.split()
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Command failed: {' '.join(cmd)}")
        logger.error(f"STDERR: {result.stderr}")
        return None
    return result.stdout

def wait_for_flink():
    """Wait for Flink REST API to be ready."""
    logger.info("Waiting for Flink JobManager REST API...")
    for _ in range(30):
        try:
            resp = requests.get(f"{FLINK_REST_API}/overview", timeout=2)
            if resp.status_code == 200:
                logger.info("Flink REST API is ready.")
                return True
        except:
            pass
        time.sleep(2)
    logger.error("Flink did not become ready.")
    return False

def wait_for_kafka():
    """Wait for Kafka to be ready (by checking topics)."""
    logger.info("Waiting for Kafka...")
    for _ in range(30):
        try:
            # Use docker exec on Kafka container to list topics
            cmd = ["docker", "exec", "cdc-kafka", "kafka-topics", "--bootstrap-server", "localhost:9092", "--list"]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode == 0:
                logger.info("Kafka is ready.")
                return True
        except:
            pass
        time.sleep(2)
    logger.error("Kafka did not become ready.")
    return False

def create_internal_topics():
    """Create internal Kafka topics if missing."""
    topics = ["internal.capture", "internal.enrich"]
    for topic in topics:
        cmd = [
            "docker", "exec", "cdc-kafka", "kafka-topics",
            "--bootstrap-server", "localhost:9092",
            "--create", "--topic", topic,
            "--partitions", "3", "--replication-factor", "1",
            "--if-not-exists"
        ]
        subprocess.run(cmd, capture_output=True)
        logger.info(f"Ensured topic {topic} exists.")

def submit_job(job_file, job_name):
    """Submit a single Flink job using docker exec."""
    job_path = f"{JOBS_DIR}/{job_file}"
    logger.info(f"Submitting {job_name} ({job_path})...")
    output = run_docker_exec(f"flink run -py {job_path} --detached")
    if output and "Job has been submitted with JobID" in output:
        # Extract Job ID
        for line in output.splitlines():
            if "JobID" in line:
                logger.info(f"✅ {job_name} submitted. {line.strip()}")
                return True
    else:
        logger.error(f"❌ Failed to submit {job_name}. Output: {output}")
        return False
    return False

def main():
    if not wait_for_kafka():
        sys.exit(1)
    create_internal_topics()
    if not wait_for_flink():
        sys.exit(1)

    # Submit jobs sequentially
    for job_file, job_name in JOBS:
        if not submit_job(job_file, job_name):
            sys.exit(1)
        time.sleep(3)  # small gap between submissions

    logger.info("All jobs submitted successfully.")

if __name__ == "__main__":
    main()