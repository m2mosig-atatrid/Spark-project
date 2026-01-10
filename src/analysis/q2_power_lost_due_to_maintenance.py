import sys
import os
sys.path.append(os.path.abspath("src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead, sum as spark_sum
from pyspark.sql.window import Window
import glob
import matplotlib.pyplot as plt

from schemas import machine_events_schema

spark = SparkSession.builder \
    .appName("Q2_Power_Lost_Maintenance") \
    .getOrCreate()

# --------------------------------------------------
# 1. Load machine events
# --------------------------------------------------
files = glob.glob("data/machine_events/*.csv.gz")

df = spark.read \
    .schema(machine_events_schema) \
    .csv(files)

# Keep only ADD (0) and REMOVE (1) events
df_events = df.filter(col("event_type").isin([0, 1]))

# --------------------------------------------------
# 2. Determine observation window
# --------------------------------------------------
min_time = df_events.agg({"time": "min"}).collect()[0][0]
max_time = df_events.agg({"time": "max"}).collect()[0][0]

total_time_span = max_time - min_time

# --------------------------------------------------
# 3. Get CPU capacity per machine (from ADD events)
# --------------------------------------------------
df_cpu = df_events \
    .filter(col("event_type") == 0) \
    .select(
        col("machine_id"),
        col("cpu_capacity").alias("machine_cpu_capacity")
    ) \
    .dropna() \
    .dropDuplicates(["machine_id"])

# --------------------------------------------------
# 4. Compute total available computational power
# --------------------------------------------------
total_cpu_capacity = df_cpu.agg(
    spark_sum("machine_cpu_capacity")
).collect()[0][0]

total_available_power = total_cpu_capacity * total_time_span

# --------------------------------------------------
# 5. Compute downtime per machine
# --------------------------------------------------
window = Window.partitionBy("machine_id").orderBy("time")

df_with_next = df_events \
    .withColumn("next_time", lead("time").over(window)) \
    .withColumn("next_event", lead("event_type").over(window))

# Downtime = REMOVE followed by ADD
df_downtime = df_with_next.filter(
    (col("event_type") == 1) & (col("next_event") == 0)
)

df_downtime = df_downtime.withColumn(
    "downtime",
    col("next_time") - col("time")
)

# --------------------------------------------------
# 6. Compute lost computational power
# --------------------------------------------------
df_lost = df_downtime.join(
    df_cpu,
    on="machine_id",
    how="inner"
)

df_lost = df_lost.withColumn(
    "lost_power",
    col("downtime") * col("machine_cpu_capacity")
)

total_lost_power = df_lost.agg(
    spark_sum("lost_power")
).collect()[0][0]

# --------------------------------------------------
# 7. Compute percentage lost
# --------------------------------------------------
percentage_lost = (total_lost_power / total_available_power) * 100

print(f"Total available computational power: {total_available_power}")
print(f"Total lost computational power: {total_lost_power}")
print(f"Percentage of computational power lost due to maintenance: {percentage_lost:.2f}%")

labels = ["Available Power", "Lost Power (Maintenance)"]
values = [total_available_power - total_lost_power, total_lost_power]

plt.figure()
plt.pie(
    values,
    labels=labels,
    autopct="%1.2f%%",
    startangle=90
)
plt.title("Proportion of Computational Power Lost Due to Maintenance")

# Save the figure
plt.savefig(
    "plots/q2_power_lost_due_to_maintenance_pie.png",
    bbox_inches="tight"
)

plt.show()

spark.stop()