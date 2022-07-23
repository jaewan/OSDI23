import ray
import csv
import numpy as np 
import time
import random
import os
from time import perf_counter
from termcolor import colored
from utils.common import *

params = get_params()
OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_TRIAL = params['NUM_TRIAL']
NUM_WORKER = params['NUM_WORKER']
SEED = params['SEED']
LATENCY = params['LATENCY']
OBJECT_STORE_BUFFER_SIZE = 5_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata


def sleep_between_batches():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        '''
        t = perf_counter()
        print(colored(t,'cyan'))
        time.sleep(LATENCY)
        '''
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        '''
        start_time = perf_counter()
        print(start_time)
        ret_obj = ray.put(np.random.randint(2147483647, size=(OBJECT_SIZE // 8)))
        end_time = perf_counter()
        time_to_sleep = LATENCY - (end_time - start_time)
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)
        return ret_obj
        '''
        return np.zeros(OBJECT_SIZE // 8)
        
    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)
    n = num_fill_object_store * WORKING_SET_RATIO
    objs = []
    res = []

    ray_pipeline_begin = perf_counter()

    for _ in range(n):
        objs.append(producer.remote())

    time.sleep(1)

    for i in range(n):
        res.append(consumer.remote(objs[i]))

    del objs
    ray.get(res)

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin


ray_time = []
for i in range(NUM_TRIAL):
    ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)
    warmup(OBJECT_STORE_SIZE)

    ray_time.append(sleep_between_batches())
    print(ray_time)
    os.system('ray memory --stats-only')
    ray.shutdown()

'''
data = [np.std(ray_time), np.var(ray_time), WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)
'''

print(ray_time)
#print(f"Ray Pipieline time: {sum(ray_time)/NUM_TRIAL}")
print(colored(sum(ray_time)/NUM_TRIAL,'green'))
