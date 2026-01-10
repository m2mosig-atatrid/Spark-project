from pyspark.sql.types import (
    StructType, StructField,
    LongType, IntegerType, StringType, FloatType, BooleanType
)

machine_events_schema = StructType([
    StructField("time", LongType(), False),
    StructField("machine_id", LongType(), False),
    StructField("event_type", IntegerType(), False),
    StructField("platform_id", StringType(), True),
    StructField("cpu_capacity", FloatType(), True),
    StructField("memory_capacity", FloatType(), True)
])

job_events_schema = StructType([
    StructField("time", LongType(), False),
    StructField("missing_info", IntegerType(), True),
    StructField("job_id", LongType(), False),
    StructField("event_type", IntegerType(), False),
    StructField("user", StringType(), True),
    StructField("scheduling_class", IntegerType(), True),
    StructField("job_name", StringType(), True),
    StructField("logical_job_name", StringType(), True)
])

task_events_schema = StructType([
    StructField("time", LongType(), False),
    StructField("missing_info", IntegerType(), True),
    StructField("job_id", LongType(), False),
    StructField("task_index", IntegerType(), False),
    StructField("machine_id", LongType(), True),
    StructField("event_type", IntegerType(), False),
    StructField("user", StringType(), True),
    StructField("scheduling_class", IntegerType(), True),
    StructField("priority", IntegerType(), False),
    StructField("cpu_request", FloatType(), True),
    StructField("memory_request", FloatType(), True),
    StructField("disk_space_request", FloatType(), True),
    StructField("different_machines_constraint", BooleanType(), True)
])