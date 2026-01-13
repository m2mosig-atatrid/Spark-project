import time

def measure_execution_time(func):
    start = time.time()
    func()
    end = time.time()
    return end - start
