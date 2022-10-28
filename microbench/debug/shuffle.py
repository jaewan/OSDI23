import ray
import csv
import argparse
import numpy as np
import time
import os
from time import perf_counter
from termcolor import colored

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=100_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/shuffle.csv")
parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=10)
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=60)
parser.add_argument('--LATENCY', '-l', type=float, default=0)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_STAGES = params['NUM_STAGES']
NUM_TRIAL = params['NUM_TRIAL']
NUM_WORKER = params['NUM_WORKER']
LATENCY = params['LATENCY']
OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def warmup():
    @ray.remote(num_cpus=1)
    def producer():
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*NUM_WORKER)))

    ref = []
    for i in range(NUM_WORKER):
        ref.append(producer.remote())
    ray.get(ref[-1])
    time.sleep(0.5)
    del ref

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

    refs = []
    npartitions = (OBJECT_STORE_SIZE*WORKING_SET_RATIO)//OBJECT_SIZE

    refs.append(slow_map.options(num_returns=npartitions).remote(npartitions))
    for _ in range(npartitions-1):
        refs.append(map.options(num_returns=npartitions).remote(npartitions))

    results = []
    for j in range(npartitions):
        results.append(reduce.remote(*[ref[j] for ref in refs]))
    del refs
    ray.get(results)
    del results

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

def shuffle():
    @ray.remote
    def map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        time.sleep(LATENCY)
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        #time.sleep(1)
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


ray_time = []
for i in range(NUM_TRIAL):
    ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)
    #warmup()

    ray_time.append(shuffle_one_task_slow())
    os.system('ray memory --stats-only')
    ray.shutdown()

#header = ['std', 'var', 'working_set_ratio', 'object_store_size','object_size','time']
'''
data = [np.std(ray_time), np.var(ray_time), WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)
'''

print(ray_time)
print(colored(sum(ray_time)/NUM_TRIAL,'green'))

#ray.timeline("timeline.json")
