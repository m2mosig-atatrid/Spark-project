from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import glob, os
import matplotlib.pyplot as plt

from src.schemas import machine_events_schema, task_events_schema

# ----------------------------
# Spark session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("Q10_Machine_Overcommitment")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Load datasets
# ----------------------------
machine_files = glob.glob("data/machine_events/*.csv.gz")
task_files = glob.glob("data/task_events/*.csv.gz")

df_machines = (
    spark.read
    .schema(machine_events_schema)
    .csv(machine_files)
)

df_tasks = (
    spark.read
    .schema(task_events_schema)
    .csv(task_files)
)

# ----------------------------
# Machine CPU capacity (from ADD events)
# ----------------------------
df_capacity = (
    df_machines
    .filter(col("event_type") == 0)  # ADD
    .select(
        col("machine_id"),
        col("cpu_capacity")
    )
    .dropna()
    .dropDuplicates(["machine_id"])
)

# ----------------------------
# CPU requested per machine
# ----------------------------
df_requests = (
    df_tasks
    .filter(col("event_type") == 1)  # SCHEDULE
    .filter(col("machine_id").isNotNull())
    .filter(col("cpu_request").isNotNull())
    .groupBy("machine_id")
    .agg(
        spark_sum("cpu_request").alias("total_cpu_requested")
    )
)

# ----------------------------
# Compare requests vs capacity
# ----------------------------
df_joined = (
    df_requests
    .join(df_capacity, on="machine_id", how="inner")
)

df_overcommit = df_joined.withColumn(
    "is_overcommitted",
    col("total_cpu_requested") > col("cpu_capacity")
)

# ----------------------------
# Statistics
# ----------------------------
total_machines = df_joined.count()
overcommitted_machines = df_overcommit.filter(col("is_overcommitted")).count()

percentage = (overcommitted_machines / total_machines) * 100

print(f"Total machines analyzed: {total_machines}")
print(f"Over-committed machines: {overcommitted_machines}")
print(f"Percentage of over-committed machines: {percentage:.2f}%")

os.makedirs("plots", exist_ok=True)

labels = ["Over-committed machines", "Within capacity"]
values = [overcommitted_machines, total_machines - overcommitted_machines]

plt.figure()
plt.pie(
    values,
    labels=labels,
    autopct="%1.1f%%",
    startangle=90
)
plt.title("Machine CPU Over-Commitment")

plt.savefig(
    "plots/q10_machine_overcommitment_pie.png",
    bbox_inches="tight"
)
plt.close()

spark.stop()
