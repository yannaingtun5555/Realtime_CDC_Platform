#!/usr/bin/env python3
import json
import os
import subprocess
import time
import requests

FLINK_REST_API = os.getenv("FLINK_REST_API", "http://flink-jobmanager:8081")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config.json")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL_SECONDS", "30"))
SCALE_COOLDOWN = int(os.getenv("SCALE_COOLDOWN_SECONDS", "300"))
COMPOSE_PROJECT = os.getenv("COMPOSE_PROJECT", "streamlakecdc")
COMPOSE_FILE = os.getenv("COMPOSE_FILE", "/workspace/docker-compose.yml")
COMPOSE_PROJECT_DIRECTORY = os.getenv("COMPOSE_PROJECT_DIRECTORY", "/workspace")
TASKMANAGER_SERVICE = "flink-taskmanager"
MIN_TASKMANAGERS = int(os.getenv("MIN_TASKMANAGERS", "1"))
MAX_TASKMANAGERS = int(os.getenv("MAX_TASKMANAGERS", "4"))

last_scale_time = 0.0
current_taskmanagers = MIN_TASKMANAGERS


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle)


def compose_base_cmd() -> list[str]:
    return [
        "docker",
        "compose",
        "-p",
        COMPOSE_PROJECT,
        "--project-directory",
        COMPOSE_PROJECT_DIRECTORY,
        "-f",
        COMPOSE_FILE,
    ]


def get_current_taskmanagers() -> int:
    cmd = compose_base_cmd() + ["ps", "-q", TASKMANAGER_SERVICE]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return current_taskmanagers
    return len([line for line in result.stdout.strip().split("\n") if line])


def scale_taskmanagers(target: int) -> None:
    global current_taskmanagers, last_scale_time
    target = max(MIN_TASKMANAGERS, min(target, MAX_TASKMANAGERS))
    if target == current_taskmanagers:
        return
    print(f"Scaling {TASKMANAGER_SERVICE}: {current_taskmanagers} -> {target}")
    cmd = compose_base_cmd() + [
        "up",
        "--scale",
        f"{TASKMANAGER_SERVICE}={target}",
        "-d",
        "--no-recreate",
    ]
    subprocess.run(cmd, check=True)
    current_taskmanagers = target
    last_scale_time = time.time()


def fetch_running_jobs() -> dict[str, dict]:
    resp = requests.get(f"{FLINK_REST_API}/jobs/overview", timeout=5)
    resp.raise_for_status()
    running = {}
    for job in resp.json().get("jobs", []):
        if job.get("state") == "RUNNING":
            running[job.get("name")] = job
    return running


def get_job_metrics(job_id: str) -> dict:
    metrics: dict = {}
    try:
        resp = requests.get(
            f"{FLINK_REST_API}/jobs/{job_id}/metrics?get=numRecordsInPerSecond,currentInputWatermark",
            timeout=5,
        )
        resp.raise_for_status()
        for item in resp.json():
            metric_id = item.get("id")
            if metric_id == "numRecordsInPerSecond":
                metrics["input_rate"] = float(item.get("value", 0))
            if metric_id == "currentInputWatermark":
                watermark = float(item.get("value", 0))
                if watermark > 0:
                    metrics["watermark_delay_ms"] = max(0, time.time() * 1000 - watermark)
    except requests.RequestException as exc:
        print(f"Metrics error for {job_id}: {exc}")
    return metrics


def lag_score(metrics: dict) -> float:
    delay = metrics.get("watermark_delay_ms", 0)
    rate = metrics.get("input_rate", 0)
    if delay > 30000:
        return 0.9
    if delay > 10000:
        return 0.6
    if rate > 0 and delay > 5000:
        return 0.45
    return 0.2


def pipeline_health(config: dict, running_jobs: dict[str, dict]) -> tuple[float, list[str]]:
    issues: list[str] = []
    scores: list[float] = []
    for job in config.get("jobs", []):
        job_name = job["name"]
        if job_name not in running_jobs:
            issues.append(f"MISSING job: {job_name}")
            scores.append(1.0)
            continue
        job_id = running_jobs[job_name]["jid"]
        metrics = get_job_metrics(job_id)
        score = lag_score(metrics)
        scores.append(score)
        print(
            f"{job_name}: score={score:.2f} "
            f"rate={metrics.get('input_rate', 0):.2f}/s "
            f"lag={metrics.get('watermark_delay_ms', 0):.0f}ms"
        )
    aggregate = max(scores) if scores else 1.0
    return aggregate, issues


def main() -> None:
    global current_taskmanagers, last_scale_time
    print("Pipeline watcher started (multi-job health + TM autoscale).")
    config = load_config()
    jobs = config.get("jobs", [])
    if not jobs:
        print("No jobs in config.")
        return

    current_taskmanagers = get_current_taskmanagers()
    print(f"Initial TaskManagers: {current_taskmanagers}")

    while True:
        try:
            running_jobs = fetch_running_jobs()
        except requests.RequestException as exc:
            print(f"Flink API unavailable: {exc}")
            time.sleep(CHECK_INTERVAL)
            continue

        score, issues = pipeline_health(config, running_jobs)
        for issue in issues:
            print(f"WARNING: {issue}")

        now = time.time()
        if now - last_scale_time >= SCALE_COOLDOWN:
            if score > 0.6 and current_taskmanagers < MAX_TASKMANAGERS:
                scale_taskmanagers(current_taskmanagers + 1)
            elif score < 0.3 and current_taskmanagers > MIN_TASKMANAGERS:
                scale_taskmanagers(current_taskmanagers - 1)
        else:
            remaining = SCALE_COOLDOWN - (now - last_scale_time)
            print(f"Scale cooldown: {remaining:.0f}s remaining")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
