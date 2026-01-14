import pandas as pd
import glob

def run_q4_pandas():
    job_files = glob.glob("data/job_events/*.csv.gz")
    task_files = glob.glob("data/task_events/*.csv.gz")

    df_jobs = pd.concat(
        [pd.read_csv(f, header=None, nrows=500_000) for f in job_files],
        ignore_index=True
    )

    df_tasks = pd.concat(
        [pd.read_csv(f, header=None, nrows=500_000) for f in task_files],
        ignore_index=True
    )

    # Columns based on schema.csv
    df_jobs.columns = [
        "time", "missing", "job_id", "event_type",
        "user", "scheduling_class", "job_name", "logical_job_name"
    ]

    df_tasks.columns = [
        "time", "missing", "job_id", "task_index", "machine_id",
        "event_type", "user", "scheduling_class", "priority",
        "cpu_request", "memory_request", "disk_request",
        "constraint"
    ]

    df_jobs = df_jobs.dropna(subset=["scheduling_class"])
    df_tasks = df_tasks.dropna(subset=["scheduling_class"])

    df_jobs.groupby("scheduling_class")["job_id"].nunique()
    df_tasks.groupby("scheduling_class").size()
