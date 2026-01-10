import sys
import os
sys.path.append(os.path.abspath("src"))

from pyspark.sql import SparkSession
import glob
from schemas import job_events_schema

spark = SparkSession.builder \
    .appName("LoadJobEvents") \
    .getOrCreate()

files = glob.glob("data/job_events/*.csv.gz")

df_machine = spark.read \
    .schema(job_events_schema) \
    .csv(files)

df_machine.printSchema()
df_machine.show(5)

spark.stop()