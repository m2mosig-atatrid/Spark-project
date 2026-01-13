from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
import matplotlib.pyplot as plt
import os
import glob

from src.schemas import task_events_schema

# ----------------------------
# Spark session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("Q6_Eviction_Probability_By_Scheduling_Class")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Load task events
# ----------------------------
task_files = glob.glob("data/task_events/*.csv.gz")

df_tasks = (
    spark.read
    .schema(task_events_schema)
    .csv(task_files)
)

# ----------------------------
# Total tasks per scheduling class
# ----------------------------
total_tasks = (
    df_tasks
    .select("scheduling_class", "job_id", "task_index")
    .distinct()
    .groupBy("scheduling_class")
    .count()
    .withColumnRenamed("count", "total_tasks")
)

# ----------------------------
# Evicted tasks per scheduling class
# ----------------------------
evicted_tasks = (
    df_tasks
    .filter(col("event_type") == 2)  # eviction
    .select("scheduling_class", "job_id", "task_index")
    .distinct()
    .groupBy("scheduling_class")
    .count()
    .withColumnRenamed("count", "evicted_tasks")
)

# ----------------------------
# Compute eviction probability
# ----------------------------
eviction_rate = (
    total_tasks
    .join(evicted_tasks, on="scheduling_class", how="left")
    .fillna(0)
    .withColumn(
        "eviction_probability",
        col("evicted_tasks") / col("total_tasks")
    )
    .orderBy("scheduling_class")
)

# ----------------------------
# Show results
# ----------------------------

eviction_rate.show(truncate=False)

# ----------------------------
# Plot
# ----------------------------
pdf = eviction_rate.toPandas()

os.makedirs("plots", exist_ok=True)

plt.figure()
plt.bar(
    pdf["scheduling_class"].astype(str),
    pdf["eviction_probability"] * 100
)

plt.xlabel("Scheduling Class")
plt.ylabel("Eviction Probability (%)")
plt.title("Task Eviction Probability by Scheduling Class")

for i, v in enumerate(pdf["eviction_probability"] * 100):
    plt.text(i, v, f"{v:.2f}%", ha="center", va="bottom")

plt.savefig("plots/q6_eviction_probability_by_scheduling_class.png")
plt.close()

spark.stop()
