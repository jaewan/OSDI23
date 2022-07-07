import ray
import csv
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
    time.sleep(0.5)


def spill_from_driver_single():
    start_time = perf_counter()

    filling_object = ray.put(np.zeros(OBJECT_STORE_SIZE//8))
    filling_object = ray.get(filling_object)
    small_object = ray.put(np.zeros(OBJECT_STORE_BUFFER_SIZE))
    small_object = ray.get(small_object)

    end_time = perf_counter()

    del filling_object
    del small_object

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
        
    start_time = perf_counter()
    
    filling_object = producer.remote(OBJECT_STORE_SIZE//8)
    filling_object = ray.get(filling_object)
    small_object = producer.remote(OBJECT_STORE_BUFFER_SIZE)
    small_object = ray.get(small_object)

    end_time = perf_counter()

    del filling_object
    del small_object

    return end_time - start_time

def spill_from_worker_multiple():
    @ray.remote(num_cpus=1) 
    def producer(size): 
        return np.zeros(size)
        
    size = OBJECT_STORE_SIZE
    obj_size = OBJECT_SIZE // 8
    filling_objects = []

    start_time = perf_counter()

    while size > 0:
        filling_objects.append(producer.remote(obj_size))
        size -= OBJECT_SIZE
    filling_objects = ray.get(filling_objects)

    small_object = producer.remote(OBJECT_STORE_BUFFER_SIZE)
    small_object = ray.get(small_object)

    end_time = perf_counter()

    del filling_objects
    del small_object

    return end_time - start_time


ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)

#Warm up tasks
#warmup()

driver_single = []
driver_multiple = []
worker_single = []
worker_multiple = []

for i in range(NUM_TRIAL):
    driver_single.append(spill_from_driver_single())
    #driver_multiple.append(spill_from_driver_multiple())
    #worker_single.append(spill_from_worker_single())
    #worker_multiple.append(spill_from_worker_multiple())

print("Driver Single: " + colored(sum(driver_single)/NUM_TRIAL,'green'))
#print("Driver Multiple: " + colored(sum(driver_multiple)/NUM_TRIAL,'green'))
#print("Worker Single: " + colored(sum(worker_single)/NUM_TRIAL,'green'))
#print("Worker Multiple: " + colored(sum(worker_multiple)/NUM_TRIAL,'green'))
