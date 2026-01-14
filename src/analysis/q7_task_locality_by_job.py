from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct
import matplotlib.pyplot as plt
import os, glob
from src.schemas import task_events_schema

# Spark session
spark = SparkSession.builder \
    .appName("Q7_Task_Locality_By_Job") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Load dataset
files = glob.glob("data/task_events/*.csv.gz")

df_tasks = spark.read \
    .schema(task_events_schema) \
    .csv(files)

# Keep only scheduling events
df_scheduled = (
    df_tasks
    .filter(col("event_type") == 1)
    .filter(col("machine_id").isNotNull())
)

job_locality = (
    df_scheduled
    .groupBy("job_id")
    .agg(
        count("*").alias("num_task_attempts"),
        countDistinct("machine_id").alias("num_machines")
    )
    .withColumn(
        "locality_ratio",
        col("num_machines") / col("num_task_attempts")
    )
)

job_locality.select("locality_ratio").describe().show()

# Plot
os.makedirs("plots", exist_ok=True)

locality_values = (
    job_locality
    .select("locality_ratio")
    .sample(fraction=0.1, seed=42)
    .toPandas()["locality_ratio"]
)

plt.figure()
plt.hist(locality_values, bins=30)
plt.xlabel("Locality ratio (machines / task attempts)")
plt.ylabel("Number of jobs")
plt.title("Task Locality Across Jobs")
plt.savefig("plots/q7_task_locality_distribution.png", bbox_inches="tight")
plt.close()

spark.stop()

