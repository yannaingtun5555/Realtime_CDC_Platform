#!/usr/bin/env python3
import os
import json
import requests
import subprocess
import time
import re

# ========== ENVIRONMENT VARIABLES ==========
FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
CONFIG_FILE = os.getenv("JOB_CONFIG_FILE", "/opt/flink/watcher-conf/job-config.json")
SAVEPOINT_BASE_DIR = os.getenv("SAVEPOINT_BASE_DIR", "s3a://flink-savepoints")
SAVEPOINT_INTERVAL = int(os.getenv("SAVEPOINT_INTERVAL_SECONDS", "300"))  # 5 minutes
DEFAULT_CHECK_INTERVAL = 30
MINIO_ALIAS = "local"

# ========== HELPER FUNCTIONS ==========
def load_job_configs():
    try:
        with open(CONFIG_FILE, 'r') as f:
            data = json.load(f)
            return data.get("jobs", [])
    except Exception as e:
        print(f"Error loading config: {e}")
        return []

def get_job_status(job_name):
    """Return the first RUNNING job matching the name, or None."""
    try:
        resp = requests.get(f"{FLINK_REST_API}/jobs/overview", timeout=5)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        matching = [j for j in jobs if j.get("name") == job_name and j.get("state") == "RUNNING"]
        return matching[0] if matching else None
    except Exception as e:
        print(f"Error checking jobs for {job_name}: {e}")
        return None

def take_savepoint_rest(job_id, job_name):
    """
    Trigger a savepoint via Flink REST API and wait for completion.
    Returns the savepoint path or None.
    """
    savepoint_dir = f"{SAVEPOINT_BASE_DIR}/{job_name.replace(' ', '_')}"
    print(f"Triggering savepoint for job {job_id} -> {savepoint_dir}")

    # 1. POST to trigger savepoint
    try:
        resp = requests.post(
            f"{FLINK_REST_API}/jobs/{job_id}/savepoints",
            json={"target-directory": savepoint_dir},
            timeout=10
        )
        resp.raise_for_status()
        trigger_id = resp.json().get("request-id")
        if not trigger_id:
            print("No trigger-id returned")
            return None
    except Exception as e:
        print(f"Failed to trigger savepoint: {e}")
        return None

    # 2. Poll for completion (up to 2 minutes)
    for _ in range(24):  # 24 * 5 = 120 seconds
        time.sleep(5)
        try:
            status_resp = requests.get(f"{FLINK_REST_API}/jobs/{job_id}/savepoints/{trigger_id}", timeout=5)
            status_resp.raise_for_status()
            data = status_resp.json()
            status = data.get("status", {}).get("id")
            if status == "COMPLETED":
                location = data.get("operation", {}).get("location")
                if location:
                    print(f"Savepoint completed: {location}")
                    return location
                else:
                    print("Savepoint completed but no location found")
                    return None
            elif status == "FAILED":
                print(f"Savepoint failed: {data}")
                return None
            # else still in progress, continue polling
        except Exception as e:
            print(f"Error polling savepoint: {e}")
            continue
    print("Savepoint polling timed out after 120 seconds")
    return None

def get_latest_savepoint(job_name):
    """
    Use `mc` to list savepoint directory and return the most recent savepoint full path.
    Returns None if none found.
    """
    dir_path = f"{MINIO_ALIAS}/{SAVEPOINT_BASE_DIR.replace('s3a://', '')}/{job_name.replace(' ', '_')}/"
    # Example: local/flink-savepoints/CDC_Ingestion_Job/
    cmd = ["mc", "ls", "--json", dir_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Cannot list savepoints: {result.stderr}")
        return None

    savepoints = []
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        try:
            entry = json.loads(line)
            # Skip folders, only files (savepoints are directories? Actually each savepoint is a folder)
            # But mc ls shows each savepoint folder as a directory entry with type 'folder'
            if entry.get('type') == 'folder':
                # Savepoint directory names are like savepoint-xxxx
                # Use lastModified for sorting
                savepoints.append((entry.get('lastModified'), entry.get('key')))
        except:
            pass
    if not savepoints:
        return None
    savepoints.sort(reverse=True)  # newest first
    latest_key = savepoints[0][1]
    # Convert back to s3a:// path
    full_path = f"s3a://flink-savepoints/{job_name.replace(' ', '_')}/{latest_key}"
    print(f"Latest savepoint: {full_path}")
    return full_path

def submit_job(job_config, from_savepoint=None):
    """Submit a Flink job with optional savepoint and required JARs."""
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
        # Warnings are normal, but errors should be printed
        if "ERROR" in result.stderr or "Exception" in result.stderr:
            print(result.stderr.strip())
    return result.returncode == 0

def ensure_mc_alias():
    """Set up mc alias for MinIO if not already done."""
    subprocess.run([
        "mc", "alias", "set", MINIO_ALIAS, "http://minio:9000",
        os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    ], capture_output=True)

def cancel_job(job_id):
    print(f"Cancelling job {job_id}")
    subprocess.run(["flink", "cancel", job_id], capture_output=True)

# ========== MAIN LOOP ==========
def main():
    print("Job watcher started with savepoint recovery (REST API).")
    ensure_mc_alias()
    job_configs = load_job_configs()
    if not job_configs:
        print("No job configurations found. Exiting.")
        return

    last_savepoint_time = 0

    while True:
        for job in job_configs:
            job_name = job.get("name")
            current_job = get_job_status(job_name)

            if current_job:
                # Running – take periodic savepoint
                now = time.time()
                if now - last_savepoint_time >= SAVEPOINT_INTERVAL:
                    sp = take_savepoint_rest(current_job['jid'], job_name)
                    if sp:
                        print(f"Savepoint stored: {sp}")
                    else:
                        print("Savepoint attempt failed.")
                    last_savepoint_time = now
                print(f"{job_name} is active: {current_job['jid']}")
            else:
                # No running job – restart from latest savepoint
                print(f"{job_name} not running. Attempting restart from latest savepoint...")
                latest_sp = get_latest_savepoint(job_name)
                if latest_sp:
                    print(f"Found savepoint: {latest_sp}")
                else:
                    print("No savepoint found – starting fresh.")
                success = submit_job(job, from_savepoint=latest_sp)
                if success:
                    print(f"Job {job_name} submitted successfully.")
                else:
                    print(f"Failed to submit {job_name}.")
                # Reset timer after restart
                last_savepoint_time = 0
                time.sleep(10)  # avoid rapid resubmit

        time.sleep(DEFAULT_CHECK_INTERVAL)

if __name__ == "__main__":
    main()