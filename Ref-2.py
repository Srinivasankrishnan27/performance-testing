import time
import csv
import random
import statistics
from datetime import datetime
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- MongoDB Connection ---
client = MongoClient("mongodb://localhost:27017/")
db = client["finance_db"]

# --- Configuration ---
N_USERS = 130
DURATION_SEC = 60              # Continuous benchmark duration
CSV_FILE = "benchmark_results.csv"
ENABLE_EXPLAIN = True          # Set False to skip explain profiling

# --- Base Query Templates (to randomize) ---
def random_queries():
    """Return randomized queries for realism."""
    return {
        "transactions": {"transaction_id": f"T{random.randint(100000, 999999)}"},
        "customers": {"customer_id": f"C{random.randint(100, 999)}"},
        "accounts": {"account_no": f"A{random.randint(100000, 999999)}"}
    }

# --- Single user iteration (3 queries) ---
def user_task(user_id, end_time):
    user_results = []

    while time.perf_counter() < end_time:
        queries = random_queries()

        for coll_name, query in queries.items():
            coll = db[coll_name]

            start_q = time.perf_counter()
            doc = coll.find_one(query)
            elapsed_q = (time.perf_counter() - start_q) * 1000  # ms

            explain_stats = None
            if ENABLE_EXPLAIN:
                explain = coll.find(query).limit(1).explain("executionStats")
                exec_stats = explain.get("executionStats", {})
                explain_stats = {
                    "executionTimeMillis": exec_stats.get("executionTimeMillis", 0),
                    "docsExamined": exec_stats.get("totalDocsExamined", 0),
                    "indexUsed": bool(exec_stats.get("executionStages", {}).get("inputStage"))
                }

            user_results.append({
                "timestamp": datetime.utcnow().isoformat(),
                "user_id": user_id,
                "collection": coll_name,
                "query": str(query),
                "elapsed_ms": elapsed_q,
                "found": bool(doc),
                "explain_execution_ms": explain_stats["executionTimeMillis"] if explain_stats else None,
                "docs_examined": explain_stats["docsExamined"] if explain_stats else None,
                "index_used": explain_stats["indexUsed"] if explain_stats else None,
            })
    return user_results

# --- Benchmark Runner ---
def run_benchmark(n_users=N_USERS, duration_sec=DURATION_SEC):
    print(f"Starting benchmark with {n_users} users for {duration_sec} seconds...")
    end_time = time.perf_counter() + duration_sec
    results = []

    start_total = time.perf_counter()
    with ThreadPoolExecutor(max_workers=n_users) as executor:
        futures = [executor.submit(user_task, i, end_time) for i in range(n_users)]
        for future in as_completed(futures):
            results.extend(future.result())
    total_time = time.perf_counter() - start_total

    # --- Write to CSV ---
    fieldnames = [
        "timestamp", "user_id", "collection", "query",
        "elapsed_ms", "found", "explain_execution_ms",
        "docs_examined", "index_used"
    ]
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # --- Aggregate metrics ---
    latencies = [r["elapsed_ms"] for r in results]
    summary = {
        "total_users": n_users,
        "total_queries": len(latencies),
        "avg_latency_ms": statistics.mean(latencies),
        "p95_latency_ms": statistics.quantiles(latencies, n=100)[94],
        "min_latency_ms": min(latencies),
        "max_latency_ms": max(latencies),
        "total_duration_s": total_time,
        "throughput_qps": len(latencies) / total_time,
    }

    print("\n=== Benchmark Summary ===")
    for k, v in summary.items():
        print(f"{k:20}: {v:.2f}")

    print(f"\nResults saved to: {CSV_FILE}")
    return summary

# --- Run benchmark ---
if __name__ == "__main__":
    run_benchmark()
