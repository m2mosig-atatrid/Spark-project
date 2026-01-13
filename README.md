# Google Cluster Data Analysis with Apache Spark

## Overview

This project analyzes the **Google Cluster Trace dataset** using **Apache Spark (PySpark)** in order to study machine behavior, task scheduling, resource usage, and fault tolerance in a large-scale production cluster.

The analyses answer a predefined set of questions provided as part of a university lab assignment and include **two additional original analyses** that explore scheduling behavior after task evictions.

The project focuses on:
- Distributed data processing with Spark
- Event-based analysis of large datasets
- Resource management and scheduling behavior in production clusters
- Reproducible and structured analytical workflows

---

## Project Structure

Spark-project/
├── src/
│ ├── analysis/
│ │ ├── q1_cpu_capacity_distribution.py
│ │ ├── q2_power_lost_due_to_maintenance.py
│ │ ├── q3_maintenance_rate_by_cpu_class.py
│ │ ├── q4_jobs_tasks_per_scheduling_class.py
│ │ ├── q5_killed_evicted_jobs_tasks.py
│ │ ├── q6_eviction_probability_by_scheduling_class.py
│ │ ├── q7_task_locality_by_job.py
│ │ ├── q8_requested_vs_used_resources.py
│ │ ├── q9_resource_peaks_vs_evictions.py
│ │ ├── q10_machine_overcommitment.py
│ │ ├── q11_task_rescheduling_after_eviction.py
│ │ └── q12_machine_avoidance_after_eviction.py
│ └── schemas.py
│
├── data/
│ ├── machine_events/
│ ├── job_events/
│ ├── task_events/
│ └── task_usage/
│
├── plots/
│ └── (generated figures)
│
├── results/
│ └── all_results.txt
│
├── run_all.sh
├── requirements.txt
└── README.md

---

## Dataset

The project uses the **Google Cluster Trace dataset** (version 2), which contains logs describing:
- Machine lifecycle events
- Job and task scheduling events
- Resource requests and actual resource usage

Only a **subset of the full dataset** is used for practical reasons (storage and runtime constraints), while keeping representative behavior.

---

## Analyses Performed

The following analyses were conducted:

1. Distribution of machines by CPU capacity  
2. Percentage of computational power lost due to maintenance  
3. Maintenance rate by CPU capacity class  
4. Distribution of jobs and tasks by scheduling class  
5. Percentage of jobs and tasks killed or evicted  
6. Eviction probability by scheduling class  
7. Task locality within jobs  
8. Relationship between requested and consumed resources  
9. Correlation between resource usage peaks and task evictions  
10. Frequency of machine resource over-commitment  
11. Impact of task eviction on rescheduling behavior *(original)*  
12. Machine reuse avoidance after task eviction *(original)*  

Each analysis produces:
- Console results (tables or metrics)
- One or more visualizations (saved to `plots/` where relevant)

---

## Requirements

### Software prerequisites
- **Python ≥ 3.8**
- **Java (required by Apache Spark)**
- **Apache Spark 3.x**

### Python dependencies

Install required Python packages with:

```bash
pip install -r requirements.txt
```
