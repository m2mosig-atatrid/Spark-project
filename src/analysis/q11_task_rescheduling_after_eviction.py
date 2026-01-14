from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, avg
import glob, os
import matplotlib.pyplot as plt
from src.schemas import task_events_schema

# Spark session
spark = (
    SparkSession.builder
    .appName("Q11_Task_Rescheduling_After_Eviction")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Load task events
files = glob.glob("data/task_events/*.csv.gz")

df = (
    spark.read
    .schema(task_events_schema)
    .csv(files)
)

# Number of machines per task (SCHEDULE events)
task_machines = (
    df.filter(col("event_type") == 1)  # SCHEDULE
      .filter(col("machine_id").isNotNull())
      .groupBy("job_id", "task_index")
      .agg(
          countDistinct("machine_id").alias("num_machines")
      )
)

# Identify evicted tasks
evicted_tasks = (
    df.filter(col("event_type") == 2)  # EVICT
      .select("job_id", "task_index")
      .dropDuplicates()
      .withColumn("was_evicted", col("job_id").isNotNull())
)

# Join & label tasks
task_stats = (
    task_machines
    .join(evicted_tasks, ["job_id", "task_index"], "left")
    .withColumn(
        "was_evicted",
        col("was_evicted").isNotNull()
    )
)

# Aggregate comparison
result = (
    task_stats
    .groupBy("was_evicted")
    .agg(
        countDistinct("job_id", "task_index").alias("num_tasks"),
        avg("num_machines").alias("avg_machines_per_task")
    )
)

result.show()

# Visualization
os.makedirs("plots", exist_ok=True)

pdf = result.toPandas()

plt.figure()
plt.bar(
    pdf["was_evicted"].astype(str),
    pdf["avg_machines_per_task"]
)
plt.xlabel("Task evicted")
plt.ylabel("Average number of machines")
plt.title("Task Rescheduling After Eviction")

plt.savefig(
    "plots/q11_task_rescheduling_after_eviction.png",
    bbox_inches="tight"
)
plt.close()

spark.stop()
