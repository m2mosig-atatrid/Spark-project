import sys
import os

sys.path.append(os.path.abspath("src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import glob
import matplotlib.pyplot as plt
from schemas import machine_events_schema

spark = SparkSession.builder \
    .appName("Q1_CPU_Distribution") \
    .getOrCreate()

# Load machine events
files = glob.glob("data/machine_events/*.csv.gz")

df_machine = spark.read \
    .schema(machine_events_schema) \
    .csv(files)

# Keep only machine ADD events
df_add = df_machine.filter(col("event_type") == 0)

# Select CPU capacity
df_cpu = df_add.select("cpu_capacity").dropna()

# Basic statistics
df_cpu.describe().show()

cpu_values = df_cpu.toPandas()["cpu_capacity"]

plt.hist(cpu_values, bins=20)
plt.xlabel("CPU capacity")
plt.ylabel("Number of machines")
plt.title("Distribution of CPU capacity across machines")
plt.show()

spark.stop()