#!/usr/bin/env python3
import os
import json
import requests
import subprocess
import time
import re
from datetime import datetime

FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
CONFIG_FILE = os.getenv("JOB_CONFIG_FILE", "/opt/flink/watcher-conf/job-config.json")
SAVEPOINT_BASE_DIR = os.getenv("SAVEPOINT_BASE_DIR", "s3a://flink-savepoints")
SAVEPOINT_INTERVAL = int(os.getenv("SAVEPOINT_INTERVAL_SECONDS", "300"))  # 5 minutes
DEFAULT_CHECK_INTERVAL = 30

def load_job_configs():
    try:
        with open(CONFIG_FILE, 'r') as f:
            data = json.load(f)
            return data.get("jobs", [])
    except Exception as e:
        print(f"Error loading config: {e}")
        return []

def get_job_status(job_name):
    try:
        resp = requests.get(f"{FLINK_REST_API}/jobs/overview", timeout=5)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        matching = [j for j in jobs if j.get("name") == job_name]
        active = [j for j in matching if j.get("state") in {"RUNNING"}]
        return active[0] if active else None
    except Exception as e:
        print(f"Error checking jobs for {job_name}: {e}")
        return None

def take_savepoint(job_id, job_name):
    savepoint_dir = f"{SAVEPOINT_BASE_DIR}/{job_name.replace(' ', '_')}"
    print(f"Taking savepoint for job {job_id} -> {savepoint_dir}")
    cmd = ["flink", "savepoint", job_id, savepoint_dir]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Savepoint failed: {result.stderr}")
        return None
    match = re.search(r"Path: (\S+)", result.stdout)
    if match:
        savepoint_path = match.group(1)
        print(f"Savepoint taken at {savepoint_path}")
        return savepoint_path
    print(f"Could not parse savepoint path from: {result.stdout}")
    return None

def get_latest_savepoint(job_name):
    """Use mc to list savepoints and return the most recent."""
    dir_path = f"{SAVEPOINT_BASE_DIR}/{job_name.replace(' ', '_')}/"
    cmd = ["mc", "ls", "--json", dir_path]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Could not list savepoint directory: {result.stderr}")
        return None
    savepoints = []
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        try:
            entry = json.loads(line)
            if entry.get('type') == 'folder' or entry.get('size') == 0:
                continue
            savepoints.append((entry['lastModified'], entry['key']))
        except:
            pass
    if not savepoints:
        return None
    savepoints.sort(reverse=True)
    latest_key = savepoints[0][1]
    full_path = f"{dir_path}{latest_key}"
    print(f"Latest savepoint: {full_path}")
    return full_path

def submit_job(job_config, from_savepoint=None):
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

def cancel_job(job_id):
    print(f"Cancelling job {job_id}")
    subprocess.run(["flink", "cancel", job_id], capture_output=True)

def ensure_alias():
    subprocess.run(["mc", "alias", "set", "local", "http://minio:9000",
                    os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                    os.getenv("MINIO_SECRET_KEY", "minioadmin123")],
                   capture_output=True)

def main():
    print("Job watcher started with savepoint recovery.")
    ensure_alias()
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
                # Job is running – take periodic savepoint
                now = time.time()
                if now - last_savepoint_time >= SAVEPOINT_INTERVAL:
                    take_savepoint(current_job['jid'], job_name)
                    last_savepoint_time = now
                print(f"{job_name} is active: {current_job['jid']}")
            else:
                print(f"{job_name} not running. Attempting restart from latest savepoint...")
                latest_sp = get_latest_savepoint(job_name)
                if latest_sp:
                    print(f"Found savepoint: {latest_sp}")
                else:
                    print("No savepoint found – starting fresh.")

                # Optionally cancel any lingering instance (though none should be active)
                # We'll just submit new job
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