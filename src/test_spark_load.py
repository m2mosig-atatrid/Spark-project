from pyspark.sql import SparkSession
import glob

spark = SparkSession.builder \
    .appName("SparkLoadTest") \
    .getOrCreate()

machine_files = glob.glob("data/machine_events/*.csv.gz")
job_files = glob.glob("data/job_events/part-0023*.csv.gz")
task_files = glob.glob("data/task_events/part-0023*.csv.gz")

df_machine = spark.read.option("header", "false").csv(machine_files)
df_job = spark.read.option("header", "false").csv(job_files)
df_task = spark.read.option("header", "false").csv(task_files)

print("Machine events:", df_machine.count())
print("Job events:", df_job.count())
print("Task events:", df_task.count())

df_machine.show(3)
df_job.show(3)
df_task.show(3)

spark.stop()