import sys
import os
sys.path.append(os.path.abspath("src"))

from pyspark.sql import SparkSession
import glob
from schemas import machine_events_schema

spark = SparkSession.builder \
    .appName("LoadMachineEvents") \
    .getOrCreate()

files = glob.glob("data/machine_events/*.csv.gz")

df_machine = spark.read \
    .schema(machine_events_schema) \
    .csv(files)

df_machine.printSchema()
df_machine.show(5)

spark.stop()