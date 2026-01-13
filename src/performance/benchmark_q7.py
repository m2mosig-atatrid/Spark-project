from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
import glob, csv, os
from src.schemas import task_events_schema
from src.performance.benchmark_utils import measure_execution_time

spark = (
    SparkSession.builder
    .appName("Benchmark_Q7")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

files = glob.glob("data/task_events/*.csv.gz")
df = spark.read.schema(task_events_schema).csv(files)

def run_q7(use_cache=False):
    df_scheduled = df.filter(col("event_type") == 1).filter(col("machine_id").isNotNull())
    if use_cache:
        df_scheduled = df_scheduled.cache()
        df_scheduled.count()

    result = (
        df_scheduled
        .groupBy("job_id")
        .agg(
            countDistinct("task_index").alias("num_tasks"),
            countDistinct("machine_id").alias("num_machines")
        )
    )
    result.count()

results = []

for cache in [False, True]:
    duration = measure_execution_time(lambda: run_q7(cache))
    results.append(["Q7", cache, duration])

os.makedirs("results/performance", exist_ok=True)
with open("results/performance/q7_performance.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["question", "cache_enabled", "execution_time_sec"])
    writer.writerows(results)

spark.stop()
