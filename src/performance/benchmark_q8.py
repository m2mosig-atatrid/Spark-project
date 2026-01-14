from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time
import glob
import os
import csv

from src.schemas import task_events_schema, task_usage_schema

# ---------------------------------
# Output configuration
# ---------------------------------
OUTPUT_DIR = "results/performance"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "performance_q8.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_q8(cache_enabled: bool):
    spark = (
        SparkSession.builder
        .appName("Benchmark_Q8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ----------------------------
    # Load data
    # ----------------------------
    task_events_files = glob.glob("data/task_events/*.csv.gz")
    task_usage_files = glob.glob("data/task_usage/*.csv.gz")

    df_events = (
        spark.read
        .schema(task_events_schema)
        .csv(task_events_files)
        .select("job_id", "task_index", "cpu_request", "memory_request")
        .dropna()
    )

    df_usage = (
        spark.read
        .schema(task_usage_schema)
        .csv(task_usage_files)
        .select("job_id", "task_index", "cpu_usage", "canonical_memory_usage")
        .dropna()
    )

    # ----------------------------
    # Join requested vs used
    # ----------------------------
    df_joined = df_events.join(
        df_usage,
        on=["job_id", "task_index"],
        how="inner"
    )

    if cache_enabled:
        df_joined = df_joined.cache()
        df_joined.count()  # materialize cache

    # ----------------------------
    # Measure execution time
    # ----------------------------
    start = time.time()

    df_joined.stat.corr("cpu_request", "cpu_usage")
    df_joined.stat.corr("memory_request", "canonical_memory_usage")

    end = time.time()

    spark.stop()
    return end - start


if __name__ == "__main__":
    print("question,cache_enabled,execution_time_sec")

    results = []

    t_no_cache = run_q8(cache_enabled=False)
    print(f"Q8,False,{t_no_cache}")
    results.append(["Q8", False, t_no_cache])

    t_cache = run_q8(cache_enabled=True)
    print(f"Q8,True,{t_cache}")
    results.append(["Q8", True, t_cache])

    # ----------------------------
    # Save results to CSV
    # ----------------------------
    file_exists = os.path.isfile(OUTPUT_FILE)

    with open(OUTPUT_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["question", "cache_enabled", "execution_time_sec"])
        writer.writerows(results)
