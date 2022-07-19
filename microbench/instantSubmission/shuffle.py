import ray
import csv
import argparse
import numpy as np
import time
import os
from time import perf_counter
from termcolor import colored
from common import *

params = get_params()
OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_TRIAL = params['NUM_TRIAL']
NUM_WORKER = params['NUM_WORKER']
LATENCY = params['LATENCY']
OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def shuffle_one_task_slow():
    @ray.remote
    def slow_map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        time.sleep(10)
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def map(npartitions,id):
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
    refs = []

    refs.append(slow_map.options(num_returns=npartitions).remote(npartitions))
    for _ in range(npartitions-1):
        refs.append(map.options(num_returns=npartitions).remote(npartitions,_))

    results = []
    for j in range(npartitions):
        results.append(reduce.remote(*[ref[j] for ref in refs]))
    del refs
    ray.get(results[-1])
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
debugging = False

if 'dummy' in RESULT_PATH:
    debugging = True

for i in range(NUM_TRIAL):
    ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)
    if not debugging:
        warmup(OBJECT_STORE_SIZE)

    ray_time.append(shuffle_one_task_slow())
    os.system('ray memory --stats-only')
    ray.shutdown()

#header = ['std', 'var', 'working_set_ratio', 'object_store_size','object_size','time']
if not debugging:
    data = [np.std(ray_time), np.var(ray_time), WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(ray_time)/NUM_TRIAL]
    with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data)

print(ray_time)
print(colored(sum(ray_time)/NUM_TRIAL,'green'))

#ray.timeline("timeline.json")
