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

task_usage_schema = StructType([
    StructField("start_time", LongType(), False),
    StructField("end_time", LongType(), False),
    StructField("job_id", LongType(), False),
    StructField("task_index", IntegerType(), False),
    StructField("machine_id", LongType(), False),
    StructField("cpu_usage", FloatType(), True),                 
    StructField("canonical_memory_usage", FloatType(), True),    
    StructField("assigned_memory_usage", FloatType(), True),     
    StructField("unmapped_page_cache", FloatType(), True),       
    StructField("total_page_cache", FloatType(), True),          
    StructField("maximum_memory_usage", FloatType(), True),      
    StructField("disk_io_time", FloatType(), True),             
    StructField("local_disk_space_usage", FloatType(), True),    
    StructField("maximum_cpu_usage", FloatType(), True),         
    StructField("maximum_disk_io_time", FloatType(), True),      
    StructField("cycles_per_instruction", FloatType(), True),   
    StructField("memory_accesses_per_instruction", FloatType(), True), 
    StructField("sample_portion", FloatType(), True),          
    StructField("aggregation_type", BooleanType(), True),       
    StructField("sampled_cpu_usage", FloatType(), True)          
])

