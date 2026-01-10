from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count
import matplotlib.pyplot as plt
import os
from src.schemas import job_events_schema, task_events_schema

# -----------------------
# Spark session
# -----------------------
spark = SparkSession.builder \
    .appName("Q4_Jobs_Tasks_Per_Scheduling_Class") \
    .getOrCreate()

# -----------------------
# Load data
# -----------------------
df_jobs = spark.read \
    .schema(job_events_schema) \
    .option("recursiveFileLookup", "true") \
    .csv("data/job_events")

df_tasks = spark.read \
    .schema(task_events_schema) \
    .option("recursiveFileLookup", "true") \
    .csv("data/task_events")

# -----------------------
# Jobs per scheduling class
# -----------------------
jobs_per_class = (
    df_jobs
    .filter(col("scheduling_class").isNotNull())
    .select("job_id", "scheduling_class")
    .distinct()
    .groupBy("scheduling_class")
    .agg(countDistinct("job_id").alias("num_jobs"))
    .orderBy("scheduling_class")
)

print("\nJobs per scheduling class:")
jobs_per_class.show()

# -----------------------
# Tasks per scheduling class
# -----------------------
tasks_per_class = (
    df_tasks
    .filter(col("scheduling_class").isNotNull())
    .groupBy("scheduling_class")
    .agg(count("*").alias("num_tasks"))
    .orderBy("scheduling_class")
)

print("\nTasks per scheduling class:")
tasks_per_class.show()

# -----------------------
# Convert to Pandas for plotting
# -----------------------
jobs_pd = jobs_per_class.toPandas()
tasks_pd = tasks_per_class.toPandas()

# -----------------------
# Plot
# -----------------------
os.makedirs("plots", exist_ok=True)

plt.figure(figsize=(8, 5))
plt.bar(jobs_pd["scheduling_class"], jobs_pd["num_jobs"])
plt.xlabel("Scheduling class")
plt.ylabel("Number of jobs")
plt.title("Distribution of Jobs per Scheduling Class")
plt.savefig("plots/q4_jobs_per_scheduling_class.png")
plt.close()

plt.figure(figsize=(8, 5))
plt.bar(tasks_pd["scheduling_class"], tasks_pd["num_tasks"])
plt.xlabel("Scheduling class")
plt.ylabel("Number of tasks")
plt.title("Distribution of Tasks per Scheduling Class")
plt.savefig("plots/q4_tasks_per_scheduling_class.png")
plt.close()

spark.stop()
