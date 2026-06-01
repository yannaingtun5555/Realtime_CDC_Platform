#!/usr/bin/env python3
import os
import json
import requests
import subprocess
import time

FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
CONFIG_FILE = os.getenv("JOB_CONFIG_FILE", "/opt/flink/watcher-conf/job-config.json")
SAVEPOINT_BASE_DIR = os.getenv("SAVEPOINT_BASE_DIR", "s3a://flink-savepoints")
SAVEPOINT_INTERVAL = int(os.getenv("SAVEPOINT_INTERVAL_SECONDS", "300"))
CHECK_INTERVAL = 30
MINIO_ALIAS = "local"

def load_job_configs():
    try:
        with open(CONFIG_FILE, 'r') as f:
            data = json.load(f)
            return data.get("jobs", [])
    except Exception as e:
        print(f"Error loading config: {e}")
        return []

def get_running_job(job_name):
    try:
        resp = requests.get(f"{FLINK_REST_API}/jobs/overview", timeout=5)
        resp.raise_for_status()
        jobs = resp.json().get("jobs", [])
        for job in jobs:
            if job.get("name") == job_name and job.get("state") == "RUNNING":
                return job
    except Exception as e:
        print(f"Error checking jobs: {e}")
    return None

def take_savepoint_rest(job_id, job_name):
    savepoint_dir = f"{SAVEPOINT_BASE_DIR}/{job_name.replace(' ', '_')}"
    print(f"Triggering savepoint for {job_name} ({job_id}) -> {savepoint_dir}")
    try:
        resp = requests.post(
            f"{FLINK_REST_API}/jobs/{job_id}/savepoints",
            json={"target-directory": savepoint_dir},
            timeout=10
        )
        resp.raise_for_status()
        trigger_id = resp.json().get("request-id")
        if not trigger_id:
            return None
        # Poll for completion (max 120 seconds)
        # BUG FIX: wrap each poll iteration in try/except — a transient HTTP
        # error previously propagated out and crashed the entire watcher.
        for _ in range(24):
            time.sleep(5)
            try:
                status_resp = requests.get(
                    f"{FLINK_REST_API}/jobs/{job_id}/savepoints/{trigger_id}",
                    timeout=10,
                )
                status_resp.raise_for_status()
                data = status_resp.json()
            except requests.RequestException as poll_exc:
                print(f"Savepoint poll error (will retry): {poll_exc}")
                continue
            if data.get("status", {}).get("id") == "COMPLETED":
                location = data.get("operation", {}).get("location")
                print(f"Savepoint completed: {location}")
                return location
            elif data.get("status", {}).get("id") == "FAILED":
                print(f"Savepoint failed: {data}")
                return None
    except Exception as e:
        print(f"Savepoint error: {e}")
    return None

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
        except (json.JSONDecodeError, ValueError, KeyError):
            # BUG FIX: bare 'except' swallowed KeyboardInterrupt/SystemExit,
            # making the container impossible to stop cleanly.
            pass
    if not savepoints:
        return None
    savepoints.sort(reverse=True)
    latest_key = savepoints[0][1]
    full_path = f"{SAVEPOINT_BASE_DIR}/{job_name.replace(' ', '_')}/{latest_key.rstrip('/')}"
    print(f"Latest savepoint: {full_path}")
    return full_path

def submit_job(job_config, from_savepoint=None):
    print(f"Submitting job: {job_config['name']}")
    jobmanager = os.getenv("FLINK_JOBMANAGER", "flink-jobmanager:8081")
    cmd = ["flink", "run", "-m", jobmanager, "-d"]
    if from_savepoint:
        cmd.extend(["--fromSavepoint", from_savepoint])
    # BUG FIX: same as submitter.py — do NOT pass Iceberg/Hadoop JARs via -C
    # for Table API jobs; they are already registered via pipeline.jars inside
    # the Python script. Loading them via -C adds them to the Flink CLI JVM
    # and causes a LinkageError (hadoop-common bundles commons-cli which
    # conflicts with Flink's own commons-cli on the CLI classpath).
    if not job_config.get("table_api"):
        for jar in job_config.get("jars", []):
            cmd.extend(["-C", jar])
    cmd.extend(["-py", job_config["py_file"]])
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout.strip())
    if result.stderr:
        print(result.stderr.strip())
    return result.returncode == 0

def ensure_mc_alias():
    subprocess.run([
        "mc", "alias", "set", MINIO_ALIAS, "http://minio:9000",
        os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    ], capture_output=True)

def main():
    print("Job watcher started (savepoint recovery using REST API).")
    ensure_mc_alias()
    job_configs = load_job_configs()
    if not job_configs:
        print("No job configurations found. Exiting.")
        return

    # BUG FIX: was a single float shared across ALL jobs. With multiple jobs
    # the first job in the loop reset the timer immediately, so subsequent
    # jobs always saw a fresh timestamp and never received a savepoint.
    # Fix: track the last savepoint time independently per job name.
    last_savepoint_times: dict[str, float] = {job["name"]: 0.0 for job in job_configs}

    while True:
        for job in job_configs:
            job_name = job["name"]
            running_job = get_running_job(job_name)

            if running_job:
                now = time.time()
                if now - last_savepoint_times.get(job_name, 0.0) >= SAVEPOINT_INTERVAL:
                    take_savepoint_rest(running_job['jid'], job_name)
                    last_savepoint_times[job_name] = now
                print(f"{job_name} is active: {running_job['jid']}")
            else:
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
                # BUG FIX: was assigning to 'last_savepoint_time' (the old
                # scalar that no longer exists after the dict refactor), silently
                # creating a new local variable and leaving the per-job dict
                # unchanged — so the restarted job would immediately re-trigger
                # a savepoint on the very next watcher loop iteration.
                last_savepoint_times[job_name] = 0.0
                time.sleep(10)   # avoid rapid resubmit

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()