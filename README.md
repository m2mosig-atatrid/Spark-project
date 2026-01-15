# Google Cluster Data Analysis with Apache Spark

## Overview

This project presents an analytical study of the **Google Cluster Trace dataset (2011)** using **Apache Spark (PySpark)**.  
The objective is to understand machine behavior, task scheduling, resource management, and fault tolerance mechanisms in a large-scale production cluster.

The work was carried out as part of a **university lab on distributed data processing**.  
It addresses a predefined set of analytical questions and includes **original extensions** focusing on scheduling behavior after task evictions and performance evaluation.

The project emphasizes:
- Distributed data processing with Apache Spark
- Event-based analysis of large-scale traces
- Resource allocation and scheduling policies
- Performance evaluation and comparison of processing solutions
- Reproducible and structured experimentation

---

## Dataset

The project uses the **Google Cluster Trace dataset (version 2)**, which contains logs describing:
- Machine lifecycle events
- Job and task scheduling events
- Resource requests and actual resource usage

For practical reasons (storage and execution time), the analyses are performed on a **representative subset** of the dataset (parts 235 to 239), while preserving realistic cluster behavior.

All datasets are stored locally in compressed CSV format and loaded using **explicit Spark schemas** derived from the official `schema.csv` file.

---

## Project Structure

The project is organized as follows:

```
Spark-project/
├── src/
│ ├── analysis/ # Q1–Q12 analysis scripts
│ ├── performance/ # Performance evaluation (caching, cores)
│ ├── comparison/ # DataFrame vs RDD vs Pandas comparison
│ └── schemas.py # Explicit Spark schemas
│
├── data/ # Google Cluster Trace datasets (not submitted)
│
├── plots/ # Generated figures
│
├── results/ # Aggregated numerical outputs
│
├── run_all_analyses.sh # Script to run all analyses
├── requirements.txt # Python dependencies
└── README.md
```
---


Each analysis script is independent and can be executed separately or through the global runner script.

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
- Numerical results printed to the console
- One or more visualizations saved to the `plots/` directory

---

## Project Extensions

In addition to the required analyses, the project includes the following extensions:

### Performance Evaluation
- Application-level performance comparison with and without caching
- System-level evaluation of execution time as a function of the number of executor cores

### Comparison of Processing Solutions
- Spark DataFrame vs Spark RDD vs Pandas implementation
- Comparison based on execution time, scalability, and ease of use

These extensions are implemented in dedicated modules under `src/performance` and `src/comparison`.

---

## Requirements

### Software prerequisites
- **Python ≥ 3.8**
- **Java (required by Apache Spark)**
- **Apache Spark 3.x**

### Python dependencies

All required Python packages are listed in `requirements.txt`.  
Install them with:

```bash
pip install -r requirements.txt
```

Version ranges are used to ensure compatibility across different systems.

---

## How to run the project

### 1. Create and activate a virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```
### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Run all analyses at once 
A helper script is provided to run all analyses sequentially and store the results in a single file.
```bash
bash run_all_analyses.sh
```

This will:
- Execute all analysis scripts
- Save numerical outputs to results/all_results.txt
- Save generated plots to the plots/ directory

### 4. Run a single analysis
You can also run each analysis independently, for example:
```bash
python -m src.analysis.q1_cpu_capacity_distribution
```

---

## Output

- **Console output:** Printed metrics and tables
- **Results file:** results/all_results.txt
- **Plots:** Saved under the plots/ directory

Warnings and Spark progress logs are suppressed in the aggregated run to keep outputs readable.

---

## Notes

- Explicit schemas are used to ensure correct data typing and consistency.
- Jobs and tasks are treated as entities, while events are treated as observations, following the dataset semantics.
- Performance benchmarks are isolated from analytical logic to ensure fair comparisons.
