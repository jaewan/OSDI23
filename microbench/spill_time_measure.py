import ray
import csv
import argparse
import numpy as np 
import time
import json
import os
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=16_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=500_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=1)
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=1)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
RESULT_PATH = params['RESULT_PATH']
NUM_TRIAL = params['NUM_TRIAL']
NUM_WORKER = params['NUM_WORKER']
OBJECT_STORE_BUFFER_SIZE = 5_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def warmup():
    @ray.remote(num_cpus=1)
    def producer():
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*NUM_WORKER)))

    ref = []
    for i in range(NUM_WORKER):
        ref.append(producer.remote())
    ray.get(ref[-1])
    del ref


def simple_spill_test():
    @ray.remote(num_cpus=1)
    def spill():
        n = (OBJECT_STORE_SIZE//OBJECT_SIZE)
        obj_size = OBJECT_SIZE//8
        arr = []

        #Get base_time of creating an object
        start_time = perf_counter()
        arr.append(ray.put(np.zeros(obj_size)))
        end_time = perf_counter()
        base_time = end_time - start_time
        n = n-1

        #Fill the Object Store
        for i in range(n):
            arr.append(ray.put(np.zeros(obj_size)))

        #Trigger Spill, Measure time
        start_time = perf_counter()
        arr.append(ray.put(np.zeros(obj_size)))
        obj = ray.get(arr[-1])
        end_time = perf_counter()

        print('base time', base_time)
        print('spill time', end_time - start_time - base_time)
        return end_time - start_time - base_time

    spill_time = spill.remote()
    spill_time = ray.get(spill_time)

    os.system('ray memory --stats-only')

    return spill_time


def measure_spill_time():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def fill_object_store(): 
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8))),np.random.randint(2147483647, size=(OBJECT_STORE_BUFFER_SIZE//(16)))

    @ray.remote(num_cpus=1) 
    def trigger_spill(): 
        start_time = perf_counter()
        ret_obj = ray.put(np.random.randint(2147483647, size=(OBJECT_STORE_BUFFER_SIZE // 4)))
        end_time = perf_counter()

        ret = end_time - start_time
        return ret
        
    base_time = trigger_spill.remote()
    base_time = ray.get(base_time)

    full_obj_ref = fill_object_store.options(num_returns=2).remote()
    ray.get(full_obj_ref)

    ray_pipeline_begin = perf_counter()

    spill_time = trigger_spill.remote()
    spill_time = ray.get(spill_time)

    ray_pipeline_end = perf_counter()

    spill_time = spill_time - base_time

    consumer.remote(full_obj_ref)
    del full_obj_ref
    return spill_time


#ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)
ray.init(_system_config={
        "object_spilling_config": json.dumps(
            {"type": "filesystem", "params": {"directory_path": "/home/ubuntu/NSDI23/spill"}},
        )
    }, object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)

#Warm up tasks
#warmup()

ray_time = []
for i in range(NUM_TRIAL):
    ray_time.append(simple_spill_test())

print(f"Spill time: {sum(ray_time)/NUM_TRIAL}")
print("variance ", np.var(ray_time))
print(ray_time)
