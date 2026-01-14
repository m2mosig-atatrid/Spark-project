from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import glob
import os

from src.schemas import job_events_schema, task_events_schema

# Spark session
spark = (
    SparkSession.builder
    .appName("Q5_Killed_Evicted_Percentage")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Parameters
# Event types according to Google cluster trace:
# 2 = EVICT, 3 = FAIL, 4 = KILL
KILL_EVENTS = [2, 3, 4]

# Load datasets
job_files = glob.glob("data/job_events/*.csv.gz")
task_files = glob.glob("data/task_events/*.csv.gz")

df_jobs = spark.read.schema(job_events_schema).csv(job_files)
df_tasks = spark.read.schema(task_events_schema).csv(task_files)

# Jobs analysis
total_jobs = df_jobs.select("job_id").distinct().count()

killed_jobs = (
    df_jobs
    .filter(col("event_type").isin(KILL_EVENTS))
    .select("job_id")
    .distinct()
    .count()
)

jobs_percentage = (killed_jobs / total_jobs) * 100

# Tasks analysis
total_tasks = (
    df_tasks
    .select("job_id", "task_index")
    .distinct()
    .count()
)

killed_tasks = (
    df_tasks
    .filter(col("event_type").isin(KILL_EVENTS))
    .select("job_id", "task_index")
    .distinct()
    .count()
)

tasks_percentage = (killed_tasks / total_tasks) * 100

# Print results
print(f"Total jobs: {total_jobs}")
print(f"Jobs killed or evicted: {killed_jobs}")
print(f"Percentage of jobs killed or evicted: {jobs_percentage:.2f}%\n")

print(f"Total tasks: {total_tasks}")
print(f"Tasks killed or evicted: {killed_tasks}")
print(f"Percentage of tasks killed or evicted: {tasks_percentage:.2f}%\n")

# Plots
os.makedirs("plots", exist_ok=True)

# ---- Jobs pie chart ----
plt.figure()
plt.pie(
    [jobs_percentage, 100 - jobs_percentage],
    labels=["Killed / Evicted", "Successful"],
    autopct="%1.2f%%",
    startangle=90
)
plt.title("Jobs: Killed / Evicted vs Successful")
plt.savefig("plots/q5_jobs_killed_evicted_pie.png")
plt.close()

# ---- Tasks pie chart ----
plt.figure()
plt.pie(
    [tasks_percentage, 100 - tasks_percentage],
    labels=["Killed / Evicted", "Successful"],
    autopct="%1.2f%%",
    startangle=90
)
plt.title("Tasks: Killed / Evicted vs Successful")
plt.savefig("plots/q5_tasks_killed_evicted_pie.png")
plt.close()

spark.stop()
