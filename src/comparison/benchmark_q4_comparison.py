import time
import os
from src.comparison.q4_dataframe import run_q4_dataframe
from src.comparison.q4_rdd import run_q4_rdd
from src.comparison.q4_pandas import run_q4_pandas

# Configuration
OUTPUT_DIR = "results/comparison"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "q4_comparison_results.csv")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Benchmark helper
def benchmark(fn):
    start = time.time()
    fn()
    return round(time.time() - start, 2)

# Main
if __name__ == "__main__":
    header = "implementation,execution_time_sec"

    print(header)

    results = []

    t_df = benchmark(run_q4_dataframe)
    print(f"DataFrame,{t_df}")
    results.append(("DataFrame", t_df))

    t_rdd = benchmark(run_q4_rdd)
    print(f"RDD,{t_rdd}")
    results.append(("RDD", t_rdd))

    t_pd = benchmark(run_q4_pandas)
    print(f"Pandas,{t_pd}")
    results.append(("Pandas", t_pd))

    # Save results to file
    with open(OUTPUT_FILE, "w") as f:
        f.write(header + "\n")
        for impl, t in results:
            f.write(f"{impl},{t}\n")

    print(f"\nResults saved to: {OUTPUT_FILE}")

