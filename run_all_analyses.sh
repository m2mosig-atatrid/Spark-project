#!/bin/bash

OUTPUT_FILE="results/all_results.txt"
mkdir -p results

echo "==========================================" > $OUTPUT_FILE
echo " Google Cluster Data Analysis – Full Run " >> $OUTPUT_FILE
echo " Run date: $(date)" >> $OUTPUT_FILE
echo "==========================================" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

run_question () {
    QUESTION_NAME=$1
    MODULE_PATH=$2

    echo "==========================================" | tee -a $OUTPUT_FILE
    echo "$QUESTION_NAME" | tee -a $OUTPUT_FILE
    echo "==========================================" | tee -a $OUTPUT_FILE
    echo "" | tee -a $OUTPUT_FILE

    python -m $MODULE_PATH 2>/dev/null | tee -a $OUTPUT_FILE

    echo "" | tee -a $OUTPUT_FILE
}

# Activate virtual environment
# source venv/bin/activate

run_question "Q1 – CPU Capacity Distribution" src.analysis.q1_cpu_capacity_distribution
run_question "Q2 – Power Lost Due to Maintenance" src.analysis.q2_power_lost_due_to_maintenance
run_question "Q3 – Maintenance Rate by CPU Class" src.analysis.q3_maintenance_rate_by_cpu_class
run_question "Q4 – Jobs / Tasks per Scheduling Class" src.analysis.q4_jobs_tasks_per_scheduling_class
run_question "Q5 – Jobs and Tasks Killed or Evicted" src.analysis.q5_killed_evicted_jobs_tasks
run_question "Q6 – Eviction Probability by Scheduling Class" src.analysis.q6_eviction_probability_by_scheduling_class
run_question "Q7 – Task Locality Within Jobs" src.analysis.q7_task_locality_by_job
run_question "Q8 – Requested vs Consumed Resources" src.analysis.q8_requested_vs_used_resources
run_question "Q9 – Resource Peaks vs Evictions" src.analysis.q9_resource_peaks_vs_evictions
run_question "Q10 – Machine Over-Commitment" src.analysis.q10_machine_overcommitment
run_question "Q11 – Task Rescheduling After Eviction" src.analysis.q11_task_rescheduling_after_eviction
run_question "Q12 – Machine Reuse After Eviction" src.analysis.q12_machine_avoidance_after_eviction

echo "==========================================" >> $OUTPUT_FILE
echo " End of analysis run" >> $OUTPUT_FILE
echo "==========================================" >> $OUTPUT_FILE
