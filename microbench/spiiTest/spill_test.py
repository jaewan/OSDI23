import ray
import csv
import json
import argparse
import numpy as np 
import time
from time import perf_counter
import os
from termcolor import colored

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=100_000_000)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=1)
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=60)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
NUM_TRIAL = params['NUM_TRIAL']
NUM_WORKER = params['NUM_WORKER']
OBJECT_STORE_BUFFER_SIZE = 1_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def warmup():
    @ray.remote(num_cpus=1)
    def producer():
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*NUM_WORKER)))

    @ray.remote(num_cpus=1)
    def consumer(obj):
        return True

    res = []
    for i in range(NUM_WORKER):
        res.append(consumer.remote(producer.remote()))
    ray.get(res)
    del res
    time.sleep(5)

def spill_from_shuffle():
    @ray.remote
    def map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    WORKING_SET_RATIO = 4

    shuffle_start = perf_counter()

    npartitions = (OBJECT_STORE_SIZE*WORKING_SET_RATIO)//OBJECT_SIZE 
    refs = [map.options(num_returns=npartitions).remote(npartitions)
            for _ in range(npartitions)]

    res = []
    for i in range(npartitions):
        res.append(ray.get(refs[i]))
    del refs
    del res

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

def spill_from_driver_single():

    n = 160
    obj = []
    for i in range(n):
        obj.append(ray.put(np.zeros(OBJECT_STORE_SIZE//(8*n))))
    time.sleep(5)
    res = []
    res.append(ray.get(obj[-1]))

    start_time = perf_counter()
    obj.append(ray.put(np.zeros(OBJECT_STORE_BUFFER_SIZE//8)))

    res.append(ray.get(obj[-1]))

    del obj
    del res

    end_time = perf_counter()

    return end_time - start_time

def spill_from_driver_multiple():
    size = OBJECT_STORE_SIZE
    obj_size = OBJECT_SIZE // 8
    filling_objects= []

    start_time = perf_counter()

    while size > 0:
        filling_objects.append(ray.put(np.zeros(obj_size)))
        size -= OBJECT_SIZE
    filling_objects = ray.get(filling_objects)

    small_object = ray.put(np.zeros(OBJECT_STORE_BUFFER_SIZE))
    small_object = ray.get(small_object)

    end_time = perf_counter()

    del filling_objects
    del small_object

    return end_time - start_time

def spill_from_worker_single():
    @ray.remote(num_cpus=1) 
    def producer(size): 
        return np.zeros(size)
        
    
    a = producer.remote(OBJECT_STORE_SIZE//24)
    b = producer.remote(OBJECT_STORE_SIZE//24)
    c = producer.remote(OBJECT_STORE_SIZE//24)
    time.sleep(5)

    start_time = perf_counter()
    small_object = producer.remote(OBJECT_STORE_BUFFER_SIZE)
    #small_object = producer.remote(OBJECT_STORE_BUFFER_SIZE)
    small_object = ray.get(small_object)

    end_time = perf_counter()

    del a
    del b
    del c
    del small_object

    return end_time - start_time

def spill_from_worker_multiple():
    @ray.remote(num_cpus=1) 
    def producer(size): 
        return np.zeros(size)
        
    size = OBJECT_STORE_SIZE
    obj_size = OBJECT_SIZE // 8
    filling_objects = []


    while size > 0:
        filling_objects.append(producer.remote(obj_size))
        size -= OBJECT_SIZE
    time.sleep(10)
    start_time = perf_counter()
    a = ray.get(filling_objects[-1])
    #filling_objects = ray.get(filling_objects)

    small_object = producer.remote(OBJECT_STORE_BUFFER_SIZE)
    small_object = ray.get(small_object)

    end_time = perf_counter()

    del filling_objects
    del small_object
    del a

    return end_time - start_time


#ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)

#Warm up tasks
#warmup()

driver_single = []
driver_multiple = []
worker_single = []
worker_multiple = []
shufle = []

for i in range(NUM_TRIAL):
    #shufle.append(spill_from_shuffle())
    ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER, _system_config={
        "object_spilling_config":json.dumps({"type":"filesystem", "params":{"directory_path":"/home/ubuntu/NSDI23/spill"}})})
    warmup()
    worker_multiple.append(spill_from_worker_single())
    '''
    ray.shutdown()

    ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)
    warmup()
    driver_multiple.append(spill_from_driver_multiple())
    ray.shutdown()

    ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)
    warmup()
    worker_single.append(spill_from_worker_single())
    ray.shutdown()

    ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)
    warmup()
    driver_single.append(spill_from_driver_single())
    '''
    os.system('ray memory --stats-only')
    ray.shutdown()

'''
print(shufle)
print("Shuffle: " + colored(sum(shufle)/NUM_TRIAL,'green'))
'''
print("Driver Single: " + colored(sum(driver_single)/NUM_TRIAL,'green'))
print("Driver Multiple: " + colored(sum(driver_multiple)/NUM_TRIAL,'green'))
print("Worker Single: " + colored(sum(worker_single)/NUM_TRIAL,'green'))
print("Worker Multiple: " + colored(sum(worker_multiple)/NUM_TRIAL,'green'))
