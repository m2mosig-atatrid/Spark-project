import glob, warnings, sys, os

sys.path.append(os.path.abspath("src"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
from schemas import machine_events_schema

warnings.filterwarnings("ignore")

# Spark session
spark = (
    SparkSession.builder
    .appName("Q1_CPU_Distribution")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# Load machine events
files = glob.glob("data/machine_events/*.csv.gz")

df_machine = (
    spark.read
    .schema(machine_events_schema)
    .csv(files)
)

# Keep only ADD events
df_add = df_machine.filter(col("event_type") == 0)

# One CPU value per machine
df_cpu = (
    df_add
    .select("machine_id", "cpu_capacity")
    .dropna()
    .dropDuplicates(["machine_id"])
)

# Basic statistics
df_cpu.select("cpu_capacity").describe().show()

# Plot CPU distribution
cpu_values = df_cpu.toPandas()["cpu_capacity"]

os.makedirs("plots", exist_ok=True)

plt.figure()
plt.hist(cpu_values, bins=20)
plt.xlabel("CPU capacity")
plt.ylabel("Number of machines")
plt.title("Distribution of CPU capacity across machines")

plt.savefig(
    "plots/q1_cpu_capacity_distribution.png",
    bbox_inches="tight"
)
plt.close()

spark.stop()
