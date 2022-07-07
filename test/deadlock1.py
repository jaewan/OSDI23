##########################################################
#       Deadlock#1. When all workers are spinning.
# Solution 1. Wait for ski-rental to trigger spill (X Inefficient)
# Solution 2. Trigger spill when it is detected
# Solution 3. Evict tasks when it is detected
##########################################################
import ray
import csv
import argparse
import numpy as np 
import time
import random
from time import perf_counter
from termcolor import colored

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=2)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
NUM_WORKER = params['NUM_WORKER']
OBJECT_STORE_BUFFER_SIZE = 5_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata


def all_workers_spinning1():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def fill_object_store(): 
        obj_size = (OBJECT_STORE_SIZE)//8
        return np.zeros(obj_size)

    @ray.remote(num_cpus=1) 
    def producer(): 
        obj_size = (OBJECT_STORE_SIZE//NUM_WORKER)//8
        print(obj_size)
        return np.zeros(obj_size)

    #Fill the object store
    filling_obj = fill_object_store.remote()
    time.sleep(3)

    # Submit producers so all workers are spinning
    objs = []
    for _ in range(NUM_WORKER):
        objs.append(producer.remote())

    res = consumer.remote(objs)
    print("Calling filling_obj consumer")
    r = consumer.remote(filling_obj)
    ray.get(res)
    ray.get(r)
    print("Called filling_obj consumer")

    del objs

    return True

def all_workers_spinning():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        obj_size = (OBJECT_STORE_SIZE//NUM_WORKER)//8
        print(obj_size)
        return np.zeros(obj_size)

    #Fill the object store
    filling_obj = ray.put(np.zeros(OBJECT_STORE_SIZE//8))
    time.sleep(1)

    # Submit producers so all workers are spinning
    objs = []
    for _ in range(NUM_WORKER):
        objs.append(producer.remote())

    res = consumer.remote(objs)
    consumer.remote(filling_obj)

    del objs
    ray.get(res)

    return True


ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)

print('\n**** Starting all_workers_spinning() ****')
if all_workers_spinning1():
    print("All workers spinning detected and resolved")
