from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count
import glob
from src.schemas import job_events_schema, task_events_schema

def run_q4_dataframe():
    spark = (
        SparkSession.builder
        .appName("Q4_DataFrame")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    job_files = glob.glob("data/job_events/*.csv.gz")
    task_files = glob.glob("data/task_events/*.csv.gz")

    df_jobs = (
        spark.read
        .schema(job_events_schema)
        .csv(job_files)
        .filter(col("scheduling_class").isNotNull())
        .select("job_id", "scheduling_class")
        .distinct()
        .groupBy("scheduling_class")
        .agg(countDistinct("job_id").alias("num_jobs"))
    )

    df_tasks = (
        spark.read
        .schema(task_events_schema)
        .csv(task_files)
        .filter(col("scheduling_class").isNotNull())
        .groupBy("scheduling_class")
        .agg(count("*").alias("num_tasks"))
    )

    # Trigger execution
    df_jobs.count()
    df_tasks.count()

    spark.stop()
