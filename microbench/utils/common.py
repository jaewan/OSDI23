import ray
import numpy as np 
import time
import argparse

def get_params():
    parser = argparse.ArgumentParser()
    parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
    parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
    parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=100_000_000)
    parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
    parser.add_argument('--NUM_TRIAL', '-t', type=int, default=1)
    parser.add_argument('--NUM_WORKER', '-nw', type=int, default=60)
    parser.add_argument('--SEED', '-s', type=int, default=0)
    parser.add_argument('--LATENCY', '-l', type=float, default=1.65)
    args = parser.parse_args()
    params = vars(args)
    return params

def warmup(OBJECT_STORE_SIZE):
    @ray.remote(num_cpus=1)
    def producer(n):
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*n*2)))

    @ray.remote(num_cpus=1)
    def consumer(obj):
        return True
    res = []
    n =2 
    for i in range(n):
        res.append(consumer.remote(producer.remote(n)))
    ray.get(res)
    del res
    time.sleep(1)
