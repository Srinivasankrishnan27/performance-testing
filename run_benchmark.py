import os
import csv
import time
import threading
import subprocess
import requests
import re
from statistics import mean

# --------------------------
# SETTINGS
# --------------------------
SETTINGS = {
    "ray_dashboard_url": "http://<HEAD_NODE_IP>:8265",   # ← change this
    "serve_app_name": "scam_app",                        # the Serve application name
    "deployment_name": "scam_detector",                  # deployment inside the app
    "locustfile": "locustfile.py",
    "target_host": "http://<RAY_SERVE_ENDPOINT>",        # endpoint under test
    "target_tps": 130,
    "sla_ms": 60,
    "test_duration": "10m",
    "poll_interval": 2,
    "output_csv": "benchmark_results.csv"
}

# --------------------------
# Metrics Polling
# --------------------------
metrics_history = []

def poll_replica_metrics(deployment_name: str, interval_sec: int = 2):
    global metrics_history
    dashboard_url = SETTINGS["ray_dashboard_url"].rstrip("/")
    metrics_url = f"{dashboard_url}/metrics"
    cpu_pattern = re.compile(
        rf'ray_serve_replica_cpu_percent{{.*deployment="{deployment_name}".*}} (\d+\.?\d*)'
    )
    mem_pattern = re.compile(
        rf'ray_serve_replica_mem_bytes{{.*deployment="{deployment_name}".*}} (\d+\.?\d*)'
    )

    while getattr(threading.current_thread(), "do_run", True):
        try:
            resp = requests.get(metrics_url, timeout=5)
            resp.raise_for_status()
            text = resp.text

            cpu_vals = [float(m.group(1)) for m in cpu_pattern.finditer(text)]
            mem_vals = [float(m.group(1))/1024/1024 for m in mem_pattern.finditer(text)]

            if cpu_vals and mem_vals:
                metrics_history.append({
                    "ts": time.time(),
                    "cpu_avg": mean(cpu_vals),
                    "cpu_max": max(cpu_vals),
                    "mem_avg": mean(mem_vals),
                    "mem_max": max(mem_vals)
                })
        except Exception as e:
            print(f"[WARN] metrics fetch failed: {e}")
        time.sleep(interval_sec)

# --------------------------
# Ray Serve Scaling via REST
# --------------------------
def scale_serve_deployment(app_name: str, deployment_name: str, replicas: int, cpu_per_replica: float):
    """
    Adjust Ray Serve deployment replicas and CPU allocation via REST API.
    """
    url = f"{SETTINGS['ray_dashboard_url'].rstrip('/')}/api/serve/applications/{app_name}"
    payload = {
        "deployments": [{
            "name": deployment_name,
            "num_replicas": replicas,
            "ray_actor_options": {
                "num_cpus": cpu_per_replica
            }
        }]
    }
    try:
        r = requests.patch(url, json=payload, timeout=10)
        if r.status_code not in [200, 202]:
            print(f"[WARN] Scaling request returned {r.status_code}: {r.text[:200]}")
        else:
            print(f"🧩 Scaled {deployment_name}: {replicas} replicas × {cpu_per_replica} CPU")
    except Exception as e:
        print(f"[ERROR] Failed to scale Serve deployment: {e}")
    # Wait a bit for rollout
    time.sleep(20)

# --------------------------
# Locust Runner
# --------------------------
def run_locust(replicas: int, cpu_per_replica: float):
    cmd = [
        "locust", "-f", SETTINGS["locustfile"],
        "--headless",
        "--host", SETTINGS["target_host"],
        "-u", str(SETTINGS["target_tps"]),
        "-r", str(SETTINGS["target_tps"] // 2),
        "--run-time", SETTINGS["test_duration"],
        "--csv", "locust_out",
        "--stop-timeout", "30"
    ]
    print(f"▶ Running Locust for {replicas} replicas @ {cpu_per_replica} CPU each …")
    subprocess.run(cmd, check=True)

    metrics = {"p95": None, "p99": None, "avg": None, "requests": 0}
    with open("locust_out_stats.csv", newline="") as f:
        for row in csv.DictReader(f):
            if row["Name"] == "Aggregated":
                metrics["p95"] = float(row["95%"])
                metrics["p99"] = float(row["99%"])
                metrics["avg"] = float(row["Average Response Time"])
                metrics["requests"] = int(row["Request Count"])
    return metrics

# --------------------------
# Benchmark Orchestrator
# --------------------------
def benchmark_matrix(replica_configs, cpu_configs):
    with open(SETTINGS["output_csv"], "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "replicas", "cpu_per_replica",
            "p95_ms", "p99_ms", "avg_ms",
            "cpu_avg_%", "cpu_peak_%", "mem_avg_MB", "mem_peak_MB",
            "requests", "meets_SLA"
        ])

        for replicas in replica_configs:
            for cpu_per_replica in cpu_configs:

                # 🧩 Scale Ray Serve deployment before running test
                scale_serve_deployment(
                    SETTINGS["serve_app_name"],
                    SETTINGS["deployment_name"],
                    replicas,
                    cpu_per_replica
                )

                # Reset history
                global metrics_history
                metrics_history = []

                # Start polling thread
                t = threading.Thread(
                    target=poll_replica_metrics,
                    args=(SETTINGS["deployment_name"], SETTINGS["poll_interval"])
                )
                t.do_run = True
                t.start()

                # Run load test
                metrics = run_locust(replicas, cpu_per_replica)

                # Stop polling
                t.do_run = False
                t.join(timeout=5)

                # Aggregate CPU/memory stats
                if metrics_history:
                    cpu_avg = mean(m["cpu_avg"] for m in metrics_history)
                    cpu_peak = max(m["cpu_max"] for m in metrics_history)
                    mem_avg = mean(m["mem_avg"] for m in metrics_history)
                    mem_peak = max(m["mem_max"] for m in metrics_history)
                else:
                    cpu_avg = cpu_peak = mem_avg = mem_peak = 0

                meets_sla = metrics["p99"] <= SETTINGS["sla_ms"]

                writer.writerow([
                    replicas, cpu_per_replica,
                    metrics["p95"], metrics["p99"], metrics["avg"],
                    round(cpu_avg,2), round(cpu_peak,2),
                    round(mem_avg,2), round(mem_peak,2),
                    metrics["requests"], "YES" if meets_sla else "NO"
                ])

                print(f"✅ {replicas}x{cpu_per_replica} CPU → "
                      f"p99={metrics['p99']} ms, CPUavg={cpu_avg:.1f}%, Mem={mem_avg:.1f} MB")

# --------------------------
# MAIN
# --------------------------
if __name__ == "__main__":
    replica_configs = [1, 2, 3, 4]
    cpu_configs = [0.25, 0.5, 1.0]
    benchmark_matrix(replica_configs, cpu_configs)
    print("\n🏁 Benchmark completed →", SETTINGS["output_csv"])
