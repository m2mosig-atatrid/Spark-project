from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max, count
import glob
import matplotlib.pyplot as plt
import os

from src.schemas import task_events_schema, task_usage_schema

# ----------------------------
# Spark session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("Q9_Resource_Peaks_vs_Task_Evictions")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Load datasets
# ----------------------------
task_events_files = glob.glob("data/task_events/*.csv.gz")
task_usage_files = glob.glob("data/task_usage/*.csv.gz")

df_events = (
    spark.read
    .schema(task_events_schema)
    .csv(task_events_files)
)

df_usage = (
    spark.read
    .schema(task_usage_schema)
    .csv(task_usage_files)
)

# ----------------------------
# Evictions per machine
# ----------------------------
df_evictions = (
    df_events
    .filter(col("event_type") == 2)   # EVICT events
    .filter(col("machine_id").isNotNull())
    .groupBy("machine_id")
    .agg(count("*").alias("num_evictions"))
)

# ----------------------------
# Resource usage per machine
# ----------------------------
df_resources = (
    df_usage
    .groupBy("machine_id")
    .agg(
        avg("cpu_usage").alias("avg_cpu_usage"),
        spark_max("maximum_cpu_usage").alias("max_cpu_usage"),
        avg("canonical_memory_usage").alias("avg_memory_usage")
    )
)

# ----------------------------
# Join both datasets
# ----------------------------
df_joined = (
    df_resources
    .join(df_evictions, on="machine_id", how="inner")
)

# ----------------------------
# Correlations
# ----------------------------
cpu_corr = df_joined.stat.corr("avg_cpu_usage", "num_evictions")
cpu_peak_corr = df_joined.stat.corr("max_cpu_usage", "num_evictions")
mem_corr = df_joined.stat.corr("avg_memory_usage", "num_evictions")

print(f"Correlation (avg CPU usage vs evictions): {cpu_corr:.4f}")
print(f"Correlation (max CPU usage vs evictions): {cpu_peak_corr:.4f}")
print(f"Correlation (avg memory usage vs evictions): {mem_corr:.4f}")

# ----------------------------
# Scatter plot (illustrative)
# ----------------------------
os.makedirs("plots", exist_ok=True)

sample_df = (
    df_joined
    .select("avg_cpu_usage", "num_evictions")
    .sample(fraction=0.1, seed=42)
    .toPandas()
)

plt.figure(figsize=(7, 5))
plt.scatter(
    sample_df["avg_cpu_usage"],
    sample_df["num_evictions"],
    alpha=0.5
)
plt.xlabel("Average CPU usage per machine")
plt.ylabel("Number of task evictions")
plt.title("CPU Usage vs Task Evictions (Sampled Machines)")
plt.grid(True)

plt.savefig(
    "plots/q9_cpu_usage_vs_evictions_scatter.png",
    bbox_inches="tight"
)
plt.close()

spark.stop()
