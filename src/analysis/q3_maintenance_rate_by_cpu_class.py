import sys
import os
sys.path.append(os.path.abspath("src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum

import glob
import matplotlib.pyplot as plt

from schemas import machine_events_schema

# --------------------------------------------------
# Spark session
# --------------------------------------------------
spark = SparkSession.builder \
    .appName("Q3_Maintenance_Rate_By_CPU_Class") \
    .getOrCreate()

# --------------------------------------------------
# Load machine events
# --------------------------------------------------
files = glob.glob("data/machine_events/*.csv.gz")

df = spark.read \
    .schema(machine_events_schema) \
    .csv(files)

# --------------------------------------------------
# Extract CPU capacity per machine (from ADD events)
# --------------------------------------------------
df_cpu = df.filter(col("event_type") == 0) \
    .select(
        col("machine_id"),
        col("cpu_capacity").alias("cpu")
    ) \
    .dropna() \
    .dropDuplicates(["machine_id"])

# --------------------------------------------------
# Count number of machines per CPU class
# --------------------------------------------------
df_machine_count = df_cpu.groupBy("cpu") \
    .agg(count("machine_id").alias("num_machines"))

# --------------------------------------------------
# Count REMOVE events per machine
# --------------------------------------------------
df_removals = df.filter(col("event_type") == 1) \
    .groupBy("machine_id") \
    .agg(count("*").alias("num_removals"))

# --------------------------------------------------
# Join removals with CPU classes
# --------------------------------------------------
df_maintenance = df_cpu.join(
    df_removals,
    on="machine_id",
    how="left"
).fillna(0)

# --------------------------------------------------
# Compute total removals per CPU class
# --------------------------------------------------
df_removals_by_cpu = df_maintenance.groupBy("cpu") \
    .agg(
        spark_sum(col("num_removals")).alias("total_removals")
    )

# --------------------------------------------------
# Compute maintenance rate
# --------------------------------------------------
df_result = df_removals_by_cpu.join(
    df_machine_count,
    on="cpu"
)

df_result = df_result.withColumn(
    "maintenance_rate",
    col("total_removals") / col("num_machines")
)

df_result.show()

# --------------------------------------------------
# Convert to Pandas for plotting
# --------------------------------------------------
pdf = df_result.select("cpu", "maintenance_rate") \
    .orderBy("cpu") \
    .toPandas()

# --------------------------------------------------
# Plot
# --------------------------------------------------
os.makedirs("plots", exist_ok=True)

plt.figure()
plt.bar(pdf["cpu"].astype(str), pdf["maintenance_rate"])
plt.xlabel("CPU Capacity")
plt.ylabel("Maintenance Rate (removals per machine)")
plt.title("Maintenance Rate by CPU Class")

plt.savefig(
    "plots/q3_maintenance_rate_by_cpu_class.png",
    bbox_inches="tight"
)

plt.show()
plt.close()

spark.stop()
