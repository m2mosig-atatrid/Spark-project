from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import matplotlib.pyplot as plt
import glob
import os

from src.schemas import task_events_schema, task_usage_schema

# ----------------------------
# Spark session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("Q8_Requested_vs_Consumed_Resources")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Load datasets
# ----------------------------
task_event_files = glob.glob("data/task_events/*.csv.gz")
task_usage_files = glob.glob("data/task_usage/*.csv.gz")

df_tasks = (
    spark.read
    .schema(task_events_schema)
    .csv(task_event_files)
)

df_usage = (
    spark.read
    .schema(task_usage_schema)
    .csv(task_usage_files)
)

# ----------------------------
# Requested resources (from task_events)
# ----------------------------
df_requests = (
    df_tasks
    .select("job_id", "task_index", "cpu_request", "memory_request")
    .dropna(subset=["cpu_request", "memory_request"])
    .dropDuplicates(["job_id", "task_index"])
)

# ----------------------------
# Consumed resources (from task_usage)
# Aggregate mean usage per task
# ----------------------------
df_consumption = (
    df_usage
    .groupBy("job_id", "task_index")
    .agg(
        avg("cpu_usage").alias("avg_cpu_usage"),
        avg("canonical_memory_usage").alias("avg_memory_usage")
    )
    .dropna()
)

# ----------------------------
# Join requests with consumption
# ----------------------------
df_joined = df_requests.join(
    df_consumption,
    on=["job_id", "task_index"],
    how="inner"
)

# ----------------------------
# Correlation analysis
# ----------------------------
cpu_corr = df_joined.stat.corr("cpu_request", "avg_cpu_usage")
mem_corr = df_joined.stat.corr("memory_request", "avg_memory_usage")

print(f"CPU request vs CPU usage correlation: {cpu_corr:.4f}")
print(f"Memory request vs Memory usage correlation: {mem_corr:.4f}")

# ----------------------------
# Visualization
# ----------------------------
os.makedirs("plots", exist_ok=True)

sample_df = (
    df_joined
    .sample(fraction=0.05, seed=42)
    .toPandas()
)

# CPU scatter plot
plt.figure()
plt.scatter(
    sample_df["cpu_request"],
    sample_df["avg_cpu_usage"],
    alpha=0.3
)
plt.xlabel("CPU Requested")
plt.ylabel("CPU Used (avg)")
plt.title("CPU Requested vs CPU Consumed")
plt.savefig("plots/q8_cpu_request_vs_usage.png", bbox_inches="tight")
plt.close()

# Memory scatter plot
plt.figure()
plt.scatter(
    sample_df["memory_request"],
    sample_df["avg_memory_usage"],
    alpha=0.3
)
plt.xlabel("Memory Requested")
plt.ylabel("Memory Used (avg)")
plt.title("Memory Requested vs Memory Consumed")
plt.savefig("plots/q8_memory_request_vs_usage.png", bbox_inches="tight")
plt.close()

spark.stop()

