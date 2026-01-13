from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lead, when
from pyspark.sql.window import Window
import glob

from src.schemas import task_events_schema

# ----------------------------
# Spark session
# ----------------------------
spark = (
    SparkSession.builder
    .appName("Q12_Machine_Avoidance_After_Eviction")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------
# Load task events
# ----------------------------
files = glob.glob("data/task_events/*.csv.gz")

df = (
    spark.read
    .schema(task_events_schema)
    .csv(files)
)

# ----------------------------
# Keep only SCHEDULE and EVICT events
# ----------------------------
df_filtered = (
    df
    .filter(col("event_type").isin([1, 2]))  # 1 = SCHEDULE, 2 = EVICT
    .filter(col("machine_id").isNotNull())
)

# ----------------------------
# Window to track next event per task
# ----------------------------
window = Window.partitionBy("job_id", "task_index").orderBy("time")

df_seq = (
    df_filtered
    .withColumn("next_event", lead("event_type").over(window))
    .withColumn("next_machine", lead("machine_id").over(window))
)

# ----------------------------
# Eviction followed by reschedule
# ----------------------------
df_reschedule = (
    df_seq
    .filter(col("event_type") == 2)        # eviction
    .filter(col("next_event") == 1)        # followed by schedule
)

# ----------------------------
# Check if machine was reused
# ----------------------------
df_result = df_reschedule.withColumn(
    "same_machine",
    col("machine_id") == col("next_machine")
)

# ----------------------------
# Aggregate results
# ----------------------------
stats = (
    df_result
    .groupBy("same_machine")
    .count()
)

total = df_result.count()

stats = stats.withColumn(
    "percentage",
    col("count") / total * 100
)

stats.show(truncate=False)

spark.stop()
