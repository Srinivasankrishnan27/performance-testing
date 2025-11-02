"""
run_benchmark.py — Ray Serve load & sizing benchmark with visualization

Features:
- Dynamic step load based on total duration & max TPS
- Periodic CPU/memory metrics polling from Ray dashboard
- P98 latency & throughput summary
- Saves summary + time-series CSV
- Generates CPU/Memory vs Time plot
- Hook for Ray Serve autoscaling

Usage:
  python run_benchmark.py --deployment scam-detector --dashboard http://<ray-dashboard-host>:8265 --csv test_data.csv
"""

import argparse
import csv
import os
import threading
import time
import re
import requests
import statistics
import matplotlib.pyplot as plt
from locust import HttpUser, task, between, LoadTestShape, env


# ---------------------------- SETTINGS ----------------------------
SETTINGS = {
    "duration_sec": 600,               # total duration (default = 10 min)
    "max_tps": 130,                    # max simulated TPS
    "metrics_interval": 2,             # poll Ray metrics every 2 sec
    "results_dir": "benchmark_results" # output dir
}
# ------------------------------------------------------------------


# --------------------- LOCUST USER DEFINITION ---------------------
class ScamDetectorUser(HttpUser):
    wait_time = between(0.05, 0.15)

    def on_start(self):
        with open(self.environment.parsed_options.csv_file, "r") as f:
            reader = csv.DictReader(f)
            self.data = list(reader)

    @task
    def predict(self):
        sample = self.data[int(time.time() * 1000) % len(self.data)]
        self.client.post("/predict", json=sample)
# ------------------------------------------------------------------


# --------------------- DYNAMIC STEP LOAD --------------------------
def generate_stages(duration_sec: int, max_tps: int, steps: int = 3):
    """Generate load stages dynamically based on duration and max_tps."""
    stage_duration = duration_sec // steps
    stages = []
    for i in range(steps):
        end_users = int(max_tps * (0.4 + 0.3 * i)) if i < steps - 1 else max_tps
        stages.append({
            "duration": stage_duration * (i + 1),
            "users": end_users,
            "spawn_rate": max(5, end_users // 10)
        })
    return stages


class DynamicStepLoadShape(LoadTestShape):
    def __init__(self):
        super().__init__()
        self.stages = generate_stages(
            SETTINGS["duration_sec"],
            SETTINGS["max_tps"],
            steps=3
        )

    def tick(self):
        run_time = self.get_run_time()
        for stage in self.stages:
            if run_time < stage["duration"]:
                return (stage["users"], stage["spawn_rate"])
        return None
# ------------------------------------------------------------------


# --------------------- METRICS POLLING ----------------------------
metrics_history = []

def poll_replica_metrics(dashboard_url: str, deployment_name: str, interval_sec: int = 2):
    """Continuously poll CPU% and memory usage for replicas during test."""
    global metrics_history
    cpu_pattern = re.compile(
        rf'ray_serve_replica_cpu_percent{{.*deployment="{deployment_name}".*}} (\d+\.?\d*)'
    )
    mem_pattern = re.compile(
        rf'ray_serve_replica_mem_bytes{{.*deployment="{deployment_name}".*}} (\d+\.?\d*)'
    )

    while getattr(threading.current_thread(), "do_run", True):
        try:
            resp = requests.get(f"{dashboard_url.rstrip('/')}/metrics", timeout=3)
            text = resp.text
            cpu_values = [float(m.group(1)) for m in cpu_pattern.finditer(text)]
            mem_values = [float(m.group(1))/1024/1024 for m in mem_pattern.finditer(text)]  # MB

            if cpu_values or mem_values:
                metrics_history.append({
                    "timestamp": time.time(),
                    "cpu_avg": sum(cpu_values)/len(cpu_values) if cpu_values else 0,
                    "cpu_max": max(cpu_values) if cpu_values else 0,
                    "mem_avg": sum(mem_values)/len(mem_values) if mem_values else 0,
                    "mem_max": max(mem_values) if mem_values else 0,
                })
        except Exception as e:
            print(f"[WARN] Failed to fetch metrics: {e}")
        time.sleep(interval_sec)
# ------------------------------------------------------------------


# --------------------- PLOTTING FUNCTION --------------------------
def plot_metrics(metrics_history, out_path):
    """Plot CPU and Memory usage vs Time."""
    times = [m["timestamp"] - metrics_history[0]["timestamp"] for m in metrics_history]
    cpu_avg = [m["cpu_avg"] for m in metrics_history]
    cpu_max = [m["cpu_max"] for m in metrics_history]
    mem_avg = [m["mem_avg"] for m in metrics_history]
    mem_max = [m["mem_max"] for m in metrics_history]

    plt.figure(figsize=(12, 6))
    plt.subplot(2, 1, 1)
    plt.plot(times, cpu_avg, label="CPU Avg (%)")
    plt.plot(times, cpu_max, label="CPU Max (%)", linestyle="--")
    plt.ylabel("CPU Usage (%)")
    plt.legend()
    plt.grid(True)

    plt.subplot(2, 1, 2)
    plt.plot(times, mem_avg, label="Mem Avg (MB)")
    plt.plot(times, mem_max, label="Mem Max (MB)", linestyle="--")
    plt.xlabel("Time (s)")
    plt.ylabel("Memory (MB)")
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
# ------------------------------------------------------------------


# --------------------- MAIN BENCHMARK FUNCTION --------------------
def run_benchmark(deployment_name: str, dashboard_url: str, csv_file: str):
    os.makedirs(SETTINGS["results_dir"], exist_ok=True)

    print(f"\n🚀 Starting benchmark for deployment: {deployment_name}")
    print(f"📈 Duration: {SETTINGS['duration_sec']}s | Max TPS: {SETTINGS['max_tps']}\n")

    stages = generate_stages(SETTINGS["duration_sec"], SETTINGS["max_tps"])
    for i, s in enumerate(stages):
        print(f"  Stage {i+1}: up to {s['users']} users for {s['duration']}s")

    # Start metrics polling
    poll_thread = threading.Thread(
        target=poll_replica_metrics,
        args=(dashboard_url, deployment_name, SETTINGS["metrics_interval"]),
        daemon=True
    )
    poll_thread.do_run = True
    poll_thread.start()

    # ---- Run Locust ----
    env_obj = env.Environment(user_classes=[ScamDetectorUser], shape_class=DynamicStepLoadShape)
    env_obj.create_local_runner()
    env_obj.runner.start_shape()
    env_obj.parsed_options = argparse.Namespace(csv_file=csv_file)
    env_obj.create_web_ui("127.0.0.1", 8089)
    env_obj.runner.greenlet.join(timeout=SETTINGS["duration_sec"])
    env_obj.runner.quit()

    # ---- Stop metrics polling ----
    poll_thread.do_run = False
    poll_thread.join()

    # ---- Compute statistics ----
    p98_latency = env_obj.runner.stats.total.get_response_time_percentile(0.98)
    throughput = env_obj.runner.stats.total.num_requests / (SETTINGS["duration_sec"] / 60)
    avg_cpu = statistics.mean([m["cpu_avg"] for m in metrics_history])
    peak_cpu = max(m["cpu_max"] for m in metrics_history)
    avg_mem = statistics.mean([m["mem_avg"] for m in metrics_history])
    peak_mem = max(m["mem_max"] for m in metrics_history)

    summary = {
        "p98_latency_ms": p98_latency,
        "throughput_rpm": throughput,
        "cpu_avg_pct": avg_cpu,
        "cpu_peak_pct": peak_cpu,
        "mem_avg_mb": avg_mem,
        "mem_peak_mb": peak_mem
    }

    # ---- Write results ----
    hist_path = os.path.join(SETTINGS["results_dir"], "metrics_history.csv")
    plot_path = os.path.join(SETTINGS["results_dir"], "resource_usage.png")
    summary_path = os.path.join(SETTINGS["results_dir"], "summary.txt")

    with open(hist_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=metrics_history[0].keys())
        writer.writeheader()
        writer.writerows(metrics_history)

    with open(summary_path, "w") as f:
        for k, v in summary.items():
            f.write(f"{k}: {v}\n")

    # ---- Plot results ----
    plot_metrics(metrics_history, plot_path)

    print("\n✅ Benchmark completed.")
    print(f"P98 latency: {p98_latency:.2f} ms | Avg CPU: {avg_cpu:.1f}% | Peak CPU: {peak_cpu:.1f}% | Avg Mem: {avg_mem:.1f} MB")
    print(f"Results saved in: {SETTINGS['results_dir']}/")
    print(f"→ CSV: metrics_history.csv\n→ Plot: resource_usage.png\n→ Summary: summary.txt\n")

    # TODO: Optionally scale Ray Serve deployment here via Ray API

    return summary
# ------------------------------------------------------------------


# -------------------------- ENTRYPOINT -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--deployment", required=True, help="Ray Serve deployment name")
    parser.add_argument("--dashboard", required=True, help="Ray dashboard base URL (e.g. http://10.0.0.1:8265)")
    parser.add_argument("--csv", required=True, help="Path to CSV test data file")
    args = parser.parse_args()

    run_benchmark(args.deployment, args.dashboard, args.csv)
# ------------------------------------------------------------------
