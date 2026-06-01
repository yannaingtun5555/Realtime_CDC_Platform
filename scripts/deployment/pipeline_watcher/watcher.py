#!/usr/bin/env python3
import os
import json
import time
import subprocess
import requests

# ========== CONFIGURATION ==========
FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config.json")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_SECONDS", "30"))
SCALE_COOLDOWN = int(os.getenv("SCALE_COOLDOWN_SECONDS", "300"))   # avoid flapping
COMPOSE_PROJECT = os.getenv("COMPOSE_PROJECT", "streamlakecdc")
COMPOSE_FILE = os.getenv("COMPOSE_FILE", "/workspace/docker-compose.yml")
COMPOSE_PROJECT_DIRECTORY = os.getenv("COMPOSE_PROJECT_DIRECTORY", "/workspace")
TASKMANAGER_SERVICE = "flink-taskmanager"
MIN_TASKMANAGERS = int(os.getenv("MIN_TASKMANAGERS", "1"))
MAX_TASKMANAGERS = int(os.getenv("MAX_TASKMANAGERS", "4"))

# ========== GLOBAL STATE ==========
last_scale_time = 0
current_taskmanagers = MIN_TASKMANAGERS

def load_config():
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f)

def compose_base_cmd():
    return [
        "docker", "compose",
        "-p", COMPOSE_PROJECT,
        "--project-directory", COMPOSE_PROJECT_DIRECTORY,
        "-f", COMPOSE_FILE,
    ]

def get_current_taskmanagers():
    """Return current number of running TaskManager containers."""
    cmd = compose_base_cmd() + ["ps", "-q", TASKMANAGER_SERVICE]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return current_taskmanagers   # fallback
    lines = [l for l in result.stdout.strip().split('\n') if l]
    return len(lines)

def scale_taskmanagers(target):
    """Scale TaskManager service to target number of replicas."""
    global current_taskmanagers, last_scale_time
    target = max(MIN_TASKMANAGERS, min(target, MAX_TASKMANAGERS))
    if target == current_taskmanagers:
        return
    print(f"Scaling {TASKMANAGER_SERVICE} from {current_taskmanagers} to {target}")
    cmd = compose_base_cmd() + ["up", "--scale", f"{TASKMANAGER_SERVICE}={target}", "-d", "--no-recreate"]
    subprocess.run(cmd, check=True)
    current_taskmanagers = target
    last_scale_time = time.time()

def get_job_metrics(job_id):
    """Fetch a few key metrics from Flink."""
    metrics = {}
    try:
        # Get input rate (records/sec) for the source operator
        url = f"{FLINK_REST_API}/jobs/{job_id}/metrics?get=numRecordsInPerSecond"
        resp = requests.get(url, timeout=5).json()
        for m in resp:
            if m.get("id") == "numRecordsInPerSecond":
                metrics["input_rate"] = float(m.get("value", 0))
        # Get watermark delay (or use currentInputWatermark)
        url = f"{FLINK_REST_API}/jobs/{job_id}/metrics?get=currentInputWatermark"
        resp = requests.get(url, timeout=5).json()
        for m in resp:
            if m.get("id") == "currentInputWatermark":
                # Watermark value is epoch ms; delay = now - watermark
                watermark = float(m.get("value", 0))
                if watermark > 0:
                    metrics["watermark_delay_ms"] = max(0, time.time()*1000 - watermark)
        # Could also fetch Kafka consumer lag via external API or JMX – simplified here
    except Exception as e:
        print(f"Error fetching metrics: {e}")
    return metrics

def compute_score(metrics):
    """Simple score based on watermark delay. Higher = more lag."""
    delay = metrics.get("watermark_delay_ms", 0)
    # If delay > 30 seconds, consider lagging
    if delay > 30000:
        return 0.8   # high lag
    elif delay > 10000:
        return 0.5
    else:
        return 0.2   # low lag

def main():
    global current_taskmanagers, last_scale_time
    print("Pipeline watcher started. Auto‑scaling TaskManagers")
    current_taskmanagers = get_current_taskmanagers()
    config = load_config()
    jobs = config.get("jobs", [])
    if not jobs:
        print("No jobs defined, exiting.")
        return

    while True:
        # For simplicity, we consider only the first job (or aggregate)
        # In a real scenario, you might combine scores from all jobs.
        job = jobs[0]   # assume single job for now
        job_name = job["name"]
        # Find running job ID
        resp = requests.get(f"{FLINK_REST_API}/jobs/overview")
        running_jobs = resp.json().get("jobs", [])
        job_id = None
        for j in running_jobs:
            if j["name"] == job_name and j["state"] == "RUNNING":
                job_id = j["jid"]
                break
        if not job_id:
            print(f"No running job {job_name}, waiting...")
            time.sleep(CHECK_INTERVAL)
            continue

        metrics = get_job_metrics(job_id)
        score = compute_score(metrics)
        print(f"Score for {job_name}: {score:.2f} (watermark delay: {metrics.get('watermark_delay_ms',0):.0f} ms)")

        now = time.time()
        if now - last_scale_time >= SCALE_COOLDOWN:
            if score > 0.6 and current_taskmanagers < MAX_TASKMANAGERS:
                scale_taskmanagers(current_taskmanagers + 1)
            elif score < 0.3 and current_taskmanagers > MIN_TASKMANAGERS:
                scale_taskmanagers(current_taskmanagers - 1)
        else:
            print(f"Cooldown active. Next scaling allowed in {SCALE_COOLDOWN - (now - last_scale_time):.0f}s")

        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()