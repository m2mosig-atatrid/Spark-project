import time
from src.comparison.q4_dataframe import run_q4_dataframe
from src.comparison.q4_rdd import run_q4_rdd
from src.comparison.q4_pandas import run_q4_pandas

def benchmark(fn):
    start = time.time()
    fn()
    return round(time.time() - start, 2)

if __name__ == "__main__":
    print("implementation,execution_time_sec")

    t_df = benchmark(run_q4_dataframe)
    print(f"DataFrame,{t_df}")

    t_rdd = benchmark(run_q4_rdd)
    print(f"RDD,{t_rdd}")

    t_pd = benchmark(run_q4_pandas)
    print(f"Pandas,{t_pd}")
