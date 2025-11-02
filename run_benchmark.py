import json, yaml, subprocess, time, csv, os
from datetime import datetime
import psutil
import ray

# === Load Settings ===
with open("settings.yaml") as f:
    SETTINGS = yaml.safe_load(f)

TARGET_TPS = SETTINGS["target_tps"]
TEST_DURATION = SETTINGS["test_duration"]
WARMUP_DELAY = SETTINGS["warmup_delay_sec"]
STOP_TIMEOUT = SETTINGS["stop_timeout_sec"]
CONFIG_FILE = SETTINGS["config_file"]
RESULTS_DIR = SETTINGS["results_dir"]
SLA_MS = SETTINGS["sla_ms"]

os.makedirs(RESULTS_DIR, exist_ok=True)

# === Initialize Ray ===
ray.init(address="auto", ignore_reinit_error=True)

def update_ray_deployment(replicas: int, cpu: float):
    """
    Updates the Ray Serve deployment with new replica and CPU configuration.
    """
    from ray import serve
    serve_instance = serve.connect()
    deployment = serve_instance.get_deployment("scam_detector")
    deployment.options(
        num_replicas=replicas,
        ray_actor_options={"num_cpus": cpu}
    ).deploy()
    print(f"[INFO] Updated deployment: replicas={replicas}, cpu={cpu}")
    time.sleep(WARMUP_DELAY)

def run_locust(replica: int, cpu: float):
    """
    Executes a headless Locust test and extracts latency percentiles.
    """
    prefix = f"results/temp_r{replica}_c{cpu}".replace(".", "_")
    cmd = [
        "locust",
        "-f", "locustfile.py",
        "--headless",
        "-u", str(TARGET_TPS),
        "-r", str(TARGET_TPS // 10),
        "--run-time", TEST_DURATION,
        "--csv", prefix,
        "--stop-timeout", str(STOP_TIMEOUT)
    ]
    print(f"[INFO] Running Locust test: {replica} replicas × {cpu} CPU ...")
    subprocess.run(cmd, check=True)

    # Parse Locust results
    with open(f"{prefix}_stats.csv") as f:
        for row in csv.DictReader(f):
            if row["Name"] == "Total":
                return {
                    "p95": float(row["95%"]),
                    "p99": float(row["99%"]),
                    "avg": float(row["Average Response Time"]),
                    "requests": int(row["Request Count"])
                }

def get_cpu_utilization():
    """
    Returns the average CPU utilization over a short interval.
    """
    return psutil.cpu_percent(interval=2)

def compute_cluster_cpu_needed(replicas: int, cpu_per_replica: float):
    """
    Computes total CPU required including Ray and autoscaler reserves.
    """
    total = replicas * cpu_per_replica
    total += SETTINGS["autoscaler_reserve_cpu"]
    total += SETTINGS["controller_reserve_cpu"]
    return total

def main():
    with open(CONFIG_FILE) as f:
        configs = json.load(f)

    results_path = os.path.join(
        RESULTS_DIR,
        f"benchmark_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    with open(results_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "replicas", "cpu_per_replica", "p95", "p99",
            "avg_latency", "cpu_util", "requests", "total_cluster_cpu", "meets_sla"
        ])

        for cfg in configs:
            replicas, cpu = cfg["replicas"], cfg["cpu"]

            update_ray_deployment(replicas, cpu)
            metrics = run_locust(replicas, cpu)
            cpu_util = get_cpu_utilization()
            total_cpu = compute_cluster_cpu_needed(replicas, cpu)
            meets_sla = metrics["p99"] <= SLA_MS and cpu_util < 85

            writer.writerow([
                replicas, cpu, metrics["p95"], metrics["p99"], metrics["avg"],
                cpu_util, metrics["requests"], total_cpu, "YES" if meets_sla else "NO"
            ])
            print(f"[DONE] {replicas}x{cpu}: p99={metrics['p99']} ms | CPU={cpu_util}% | Meets SLA={meets_sla}")

    print(f"\n✅ Benchmark complete! Results saved to {results_path}\n")

if __name__ == "__main__":
    main()
