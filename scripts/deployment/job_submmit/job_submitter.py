#!/usr/bin/env python3
"""Submit PyFlink jobs to the Flink cluster."""

import os
import shlex
import subprocess
import time
import sys
import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
FLINK_JOBMANAGER_CONTAINER = os.getenv("FLINK_JOBMANAGER_CONTAINER", "cdc-flink-jobmanager")
KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "cdc-kafka")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
JOBS_DIR = os.getenv("JOBS_DIR", "/opt/flink/jobs")
JOB_FILES = [job.strip() for job in os.getenv("FLINK_JOB_FILES", "").split(",") if job.strip()]
#FLINK_CLASSPATH_JARS = [
#   jar.strip()
#    for jar in os.getenv(
#        "/opt/flink/lib/flink-connector-kafka-3.2.0-1.18.jar,"
#        "/opt/flink/lib/kafka-clients-3.7.0.jar",
#    ).split(",")
#    if jar.strip()
#]


def run_command(cmd):
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except FileNotFoundError:
        logger.error("Command not found: %s", cmd[0])
        return None

    if result.returncode != 0:
        logger.error("Command failed: %s", " ".join(shlex.quote(part) for part in cmd))
        if result.stdout:
            logger.error("STDOUT: %s", result.stdout.strip())
        if result.stderr:
            logger.error("STDERR: %s", result.stderr.strip())
        return None
    return result.stdout


def run_docker_exec(command):
    """Run a command inside the Flink JobManager container."""
    return run_command(["docker", "exec", FLINK_JOBMANAGER_CONTAINER] + command)


def wait_for_docker():
    """Wait until the mounted Docker socket is usable."""
    logger.info("Waiting for Docker socket...")
    for _ in range(30):
        if run_command(["docker", "version", "--format", "{{.Server.Version}}"]) is not None:
            logger.info("Docker is ready.")
            return True
        time.sleep(2)
    logger.error("Docker did not become ready.")
    return False

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
            cmd = [
                "docker", "exec", KAFKA_CONTAINER, "kafka-topics",
                "--bootstrap-server", KAFKA_BOOTSTRAP_SERVERS, "--list"
            ]
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
            "docker", "exec", KAFKA_CONTAINER, "kafka-topics",
            "--bootstrap-server", KAFKA_BOOTSTRAP_SERVERS,
            "--create", "--topic", topic,
            "--partitions", "3", "--replication-factor", "1",
            "--if-not-exists"
        ]
        if run_command(cmd) is None:
            return False
        logger.info(f"Ensured topic {topic} exists.")
    return True


def discover_jobs():
    if JOB_FILES:
        jobs = JOB_FILES
    else:
        try:
            jobs = sorted(file_name for file_name in os.listdir(JOBS_DIR) if file_name.endswith(".py"))
        except FileNotFoundError:
            logger.error("Jobs directory does not exist: %s", JOBS_DIR)
            return []

    if not jobs:
        logger.error("No PyFlink job files found in %s.", JOBS_DIR)
        return []

    logger.info("Jobs to submit: %s", ", ".join(jobs))
    return jobs


def submit_job(job_file):
    """Submit a single Flink job using docker exec."""
    job_path = f"{JOBS_DIR}/{job_file}"
    logger.info(f"Submitting {job_file} ({job_path})...")
    command = ["flink", "run", "-d", "-py", job_path]   # no -C flags
    jars = [
        "/opt/flink/lib/flink-connector-kafka-3.2.0-1.18.jar",
        "/opt/flink/lib/kafka-clients-3.2.0.jar"
    ]
    for jar in jars:
        command.extend(["-C", f"file://{jar}"])
    command.extend(["-py", job_path])
    output = run_docker_exec(command)
    if output and "Job has been submitted with JobID" in output:
        # Extract Job ID
        for line in output.splitlines():
            if "JobID" in line:
                logger.info(f"{job_file} submitted. {line.strip()}")
                return True
    else:
        logger.error(f"Failed to submit {job_file}. Output: {output}")
        return False
    return False

def main():
    if not wait_for_docker():
        sys.exit(1)
    if not wait_for_kafka():
        sys.exit(1)
    if not create_internal_topics():
        sys.exit(1)
    if not wait_for_flink():
        sys.exit(1)

    # Submit jobs sequentially
    jobs = discover_jobs()
    if not jobs:
        sys.exit(1)

    for job_file in jobs:
        if not submit_job(job_file):
            sys.exit(1)
        time.sleep(3)  # small gap between submissions

    logger.info("All jobs submitted successfully.")

if __name__ == "__main__":
    main()
