from pyspark.sql import SparkSession
import glob

def run_q4_rdd():
    spark = (
        SparkSession.builder
        .appName("Q4_RDD")
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    job_files = glob.glob("data/job_events/*.csv.gz")
    task_files = glob.glob("data/task_events/*.csv.gz")

    jobs_rdd = sc.textFile(",".join(job_files))
    tasks_rdd = sc.textFile(",".join(task_files))

    def parse_job(line):
        fields = line.split(",")
        try:
            job_id = fields[2]
            scheduling_class = fields[5]
            if scheduling_class != "":
                return (int(scheduling_class), job_id)
        except:
            return None

    def parse_task(line):
        fields = line.split(",")
        try:
            scheduling_class = fields[7]
            if scheduling_class != "":
                return (int(scheduling_class), 1)
        except:
            return None

    jobs_per_class = (
        jobs_rdd
        .map(parse_job)
        .filter(lambda x: x is not None)
        .distinct()
        .map(lambda x: (x[0], 1))
        .reduceByKey(lambda a, b: a + b)
    )

    tasks_per_class = (
        tasks_rdd
        .map(parse_task)
        .filter(lambda x: x is not None)
        .reduceByKey(lambda a, b: a + b)
    )

    # Trigger execution
    jobs_per_class.collect()
    tasks_per_class.collect()

    spark.stop()
