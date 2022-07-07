import ray
import os
import argparse
import numpy as np 
import time
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=2)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=500_000_000)
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=64)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
NUM_WORKER = params['NUM_WORKER']

def warmup():
    @ray.remote(num_cpus=1)
    def producer():
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*NUM_WORKER)))

    ref = []
    for i in range(NUM_WORKER-1):
        ref.append(producer.remote())
    ray.get(ref[-1])
    time.sleep(0.5)
    del ref

def basic_setup():
    #os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"
    os.environ["RAY_object_spilling_threshold"] = "1.0"
    os.environ["RAY_enable_BlockTasks"] = "false"
    os.environ["RAY_enable_BlockTasksSpill"] = "false"
    os.environ["RAY_enable_Deadlock1"] = "false"
    os.environ["RAY_enable_Deadlock2"] = "false"
    os.environ["RAY_spill_wait_time"] = "1000000"

    ray.init(object_store_memory=OBJECT_STORE_SIZE, num_cpus = NUM_WORKER)
    warmup()

def shuffle_one_task_slow():
    @ray.remote
    def slow_map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        time.sleep(10)
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        #time.sleep(LATENCY)
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        time.sleep(1)
        return True

    shuffle_start = perf_counter()

    npartitions = (OBJECT_STORE_SIZE*WORKING_SET_RATIO)//OBJECT_SIZE
    refs = [map.options(num_returns=npartitions).remote(npartitions)
            for _ in range(npartitions-1)]
    refs.append(slow_map.options(num_returns=npartitions).remote(npartitions))

    results = []
    for j in range(npartitions):
        results.append(reduce.remote(*[ref[j] for ref in refs]))
    del refs
    ray.get(results)
    del results

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

def test_ray_shuffle():
    @ray.remote
    def map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        time.sleep(5)
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        return True

    shuffle_start = perf_counter()

    npartitions = (OBJECT_STORE_SIZE*WORKING_SET_RATIO)//OBJECT_SIZE 
    refs = [map.options(num_returns=npartitions).remote(npartitions)
            for _ in range(npartitions)]

    results = []
    for j in range(npartitions):
        results.append(reduce.remote(*[ref[j] for ref in refs]))
    del refs
    ray.get(results)
    del results

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

def pipeline():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.zeros(OBJECT_SIZE // 8)
        
    begin = perf_counter()

    objs = []
    ret = []
    for i in range(5):
        objs.append(producer.remote())
    for i in range(5):
        ret.append(consumer.remote(objs[i]))
    del objs
    ray.get(ret)
    del ret


    end = perf_counter()

    return end - begin

def scatter():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1, num_returns=3) 
    def scatter(): 
        size = OBJECT_STORE_SIZE//8
        size = size//2
        return np.zeros(size),np.zeros(size),np.zeros(size)
        
    a, b, c = scatter.remote()
    res = []
    res.append(consumer.remote(a))
    res.append(consumer.remote(b))
    res.append(consumer.remote(c))
    del a
    del b
    del c
    ray.get(res)
    del res
    return 1

basic_setup()

print(shuffle_one_task_slow())
