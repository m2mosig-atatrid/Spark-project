from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import glob
import time
import csv
import os
import pandas as pd
import matplotlib.pyplot as plt

from src.schemas import task_events_schema, task_usage_schema

# ----------------------------
# Configuration
# ----------------------------
CORE_VALUES = [1, 2, 4, 6, 8, 12]
RESULTS_FILE = "results/performance/q8_executor_cores.csv"
PLOT_FILE = "plots/performance/q8_executor_cores.png"

os.makedirs("results/performance", exist_ok=True)
os.makedirs("plots/performance", exist_ok=True)


# ----------------------------
# Benchmark function
# ----------------------------
def run_q8_with_cores(num_cores: int) -> float:
    spark = (
        SparkSession.builder
        .appName(f"Benchmark_Q8_Cores_{num_cores}")
        .config("spark.executor.cores", num_cores)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # Load datasets
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

    df_joined = df_events.join(
        df_usage,
        on=["job_id", "task_index"],
        how="inner"
    )

    # Measure execution time
    start = time.time()

    df_joined.stat.corr("cpu_request", "cpu_usage")
    df_joined.stat.corr("memory_request", "canonical_memory_usage")

    end = time.time()

    spark.stop()
    return end - start


# ----------------------------
# Main execution
# ----------------------------
if __name__ == "__main__":
    results = []

    print("question,executor_cores,execution_time_sec")

    for cores in CORE_VALUES:
        exec_time = run_q8_with_cores(cores)
        results.append(["Q8", cores, exec_time])
        print(f"Q8,{cores},{exec_time:.2f}")

    # Save results to CSV
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["question", "executor_cores", "execution_time_sec"])
        writer.writerows(results)

    # ----------------------------
    # Plot results
    # ----------------------------
    df = pd.DataFrame(
        results,
        columns=["question", "executor_cores", "execution_time_sec"]
    )

    plt.figure(figsize=(7, 5))
    plt.plot(
        df["executor_cores"],
        df["execution_time_sec"],
        marker="o"
    )

    plt.xlabel("Number of executor cores")
    plt.ylabel("Execution time (seconds)")
    plt.title("Impact of Executor Core Count on Q8 Execution Time")
    plt.grid(True)

    plt.savefig(PLOT_FILE, bbox_inches="tight")
    plt.close()

    print(f"\nResults saved to {RESULTS_FILE}")
    print(f"Plot saved to {PLOT_FILE}")
