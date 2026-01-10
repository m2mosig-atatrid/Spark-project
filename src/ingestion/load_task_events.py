import sys
import os
sys.path.append(os.path.abspath("src"))

from pyspark.sql import SparkSession
import glob
from schemas import task_events_schema

spark = SparkSession.builder \
    .appName("LoadTaskEvents") \
    .getOrCreate()

files = glob.glob("data/task_events/*.csv.gz")

df_tasks = spark.read \
    .schema(task_events_schema) \
    .csv(files)

df_tasks.printSchema()
df_tasks.show(5)

spark.stop()