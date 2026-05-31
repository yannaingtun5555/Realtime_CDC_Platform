#!/usr/bin/env python3
import os
import json
import requests
import subprocess
import time
import re
from typing import List, Dict, Any, Optional

FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
CONFIG_FILE = os.getenv("JOB_CONFIG_FILE", "/opt/flink/watcher-conf/job-config.json")
DEFAULT_CHECK_INTERVAL = 30
SAVEPOINT_BASE_DIR = os.getenv("SAVEPOINT_BASE_DIR", "s3a://flink-savepoints")

def load_job_configs() -> List[Dict[str, Any]]:
    try:
        with open(CONFIG_FILE, 'r') as f:
            data = json.load(f)
            return data.get("jobs", [])
    except Exception as e:
        print(f"Error loading config file {CONFIG_FILE}: {e}")
        return []

def get_job_status(job_name: str) -> tuple:
    """Return (active_jobs, failed_jobs) lists for the given job name."""
    try:
        resp = requests.get(f"{FLINK_REST_API}/jobs/overview", timeout=5)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        matching = [job for job in jobs if job.get("name") == job_name]
        active = [j for j in matching if j.get("state") in {"CREATED", "RUNNING", "RESTARTING", "RECONCILING"}]
        failed = [j for j in matching if j.get("state") in {"FAILED", "CANCELED", "CANCELLING", "SUSPENDED"}]
        return active, failed
    except Exception as e:
        print(f"Error checking jobs for {job_name}: {e}")
        return [], []

def take_savepoint(job_id: str, target_dir: str) -> Optional[str]:
    """
    Trigger a savepoint for the given job and return the savepoint path.
    Runs: flink savepoint <job_id> <target_dir>
    """
    print(f"Taking savepoint for job {job_id} -> {target_dir}")
    cmd = ["flink", "savepoint", job_id, target_dir]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Savepoint failed: {result.stderr}")
        return None
    # Parse the output to get the savepoint path.
    # Example output: "Savepoint completed. Path: s3a://.../savepoint-123"
    match = re.search(r"Path: (\S+)", result.stdout)
    if match:
        savepoint_path = match.group(1)
        print(f"Savepoint taken at {savepoint_path}")
        return savepoint_path
    print(f"Could not parse savepoint path from: {result.stdout}")
    return None

def cancel_job(job_id: str):
    """Cancel a running job without taking a savepoint."""
    print(f"Cancelling job {job_id}")
    subprocess.run(["flink", "cancel", job_id], capture_output=True)

def submit_job(job_config: Dict[str, Any], from_savepoint: Optional[str] = None) -> bool:
    print(f"Submitting job: {job_config['name']}")
    cmd = ["flink", "run", "-m", "cdc-flink-jobmanager:8081", "-d"]
    if from_savepoint:
        cmd.extend(["--fromSavepoint", from_savepoint])
    for jar in job_config.get("jars", []):
        cmd.extend(["-C", jar])
    cmd.extend(["-py", job_config["py_file"]])
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())
    return result.returncode == 0

def get_latest_savepoint(job_name: str) -> Optional[str]:
    """
    List the savepoint directory for this job name and return the most recent savepoint path.
    Uses `mc` (MinIO client) to list and sort by timestamp.
    """
    savepoint_dir = f"{SAVEPOINT_BASE_DIR}/{job_name.replace(' ', '_')}/"
    print(f"Looking for latest savepoint in {savepoint_dir}")
    # Use mc to list the directory with timestamps
    cmd = ["mc", "ls", "--json", savepoint_dir]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Could not list savepoint directory: {result.stderr}")
        return None
    # Parse JSON lines from mc output
    savepoints = []
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        try:
            entry = json.loads(line)
            if entry.get('type') == 'folder' or entry.get('size') == 0:
                continue
            # entry['lastModified'] is ISO timestamp
            savepoints.append((entry['lastModified'], entry['key']))
        except:
            pass
    if not savepoints:
        return None
    savepoints.sort(reverse=True)  # newest first
    latest = savepoints[0][1]
    full_path = f"{savepoint_dir}{latest}"
    print(f"Latest savepoint: {full_path}")
    return full_path

def ensure_savepoint_dir_exists(job_name: str):
    """Create the savepoint bucket/directory if not exists (using mc)."""
    dir_path = f"{SAVEPOINT_BASE_DIR}/{job_name.replace(' ', '_')}/"
    subprocess.run(["mc", "mkdir", "--ignore-existing", dir_path], capture_output=True)

def main():
    print("Job watcher started. Loading config from", CONFIG_FILE)
    job_configs = load_job_configs()
    if not job_configs:
        print("No job configurations found. Exiting.")
        return

    # Ensure mc client is configured (for S3 access)
    # We assume the container has mc installed and configured with MINIO credentials.
    # If not, we can set alias here using environment variables.
    subprocess.run(["mc", "alias", "set", "local", "http://minio:9000", 
                    os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                    os.getenv("MINIO_SECRET_KEY", "minioadmin123")], capture_output=True)

    while True:
        for job in job_configs:
            job_name = job.get("name")
            active_jobs, failed_jobs = get_job_status(job_name)
            if active_jobs:
                # Job is healthy, nothing to do
                job_ids = ", ".join(j.get("jid", "unknown") for j in active_jobs)
                print(f"{job_name} is active: {job_ids}")
                continue

            # No active job – need to restart
            if failed_jobs:
                states = ", ".join(f"{j.get('jid')}={j.get('state')}" for j in failed_jobs)
                print(f"Found failed/stopped {job_name}: {states}. Attempting clean restart with savepoint...")
            else:
                print(f"No active {job_name} found. Attempting to start from latest savepoint...")

            # Try to get a savepoint from the old job before restarting.
            # However, the old job is already dead (failed or cancelled). 
            # In that case we cannot take a new savepoint, so we fall back to the latest existing one.
            # But if there is a failed job still present (state FAILED), we could attempt to take a savepoint?
            # Usually a failed job cannot take savepoints. So we rely on previously taken savepoints.
            # Better approach: On normal operation, periodically take savepoints (e.g., every hour) using a separate cron.
            # For simplicity now, we just look for any existing savepoint.

            latest_savepoint = get_latest_savepoint(job_name)
            if latest_savepoint:
                print(f"Found existing savepoint: {latest_savepoint}")
            else:
                print("No previous savepoint found. Will start from scratch.")

            # Cancel any leftover failed jobs (optional, but clean)
            for failed in failed_jobs:
                cancel_job(failed['jid'])

            # Ensure the savepoint directory exists
            ensure_savepoint_dir_exists(job_name)

            # Submit new job, optionally with savepoint
            success = submit_job(job, from_savepoint=latest_savepoint)
            if success:
                print(f"Job {job_name} submitted successfully.")
            else:
                print(f"Failed to submit {job_name}.")

            # Wait a bit before checking again to avoid thrashing
            time.sleep(10)

        time.sleep(DEFAULT_CHECK_INTERVAL)

if __name__ == "__main__":
    main()