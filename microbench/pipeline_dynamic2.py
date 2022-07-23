import ray
import csv
import argparse
import numpy as np 
import time
import random
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


def ray_pipeline_random():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        start_time = perf_counter()
        ret_obj = ray.put(np.random.randint(2147483647, size=(OBJECT_SIZE // 8)))
        end_time = perf_counter()
        time_to_sleep = LATENCY - (end_time - start_time)
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)
        return ret_obj
        
    random.seed(SEED)
    num_objs_to_produce = (WORKING_SET_RATIO*OBJECT_STORE_SIZE)//OBJECT_SIZE
    num_objs_to_consume = 0
    
    objs = set()
    res = []
    objs_not_consumed = 0
    idx = 0

    ray_pipeline_begin = perf_counter()

    num_first_producers = num_objs_to_produce//(random.randint(1,num_objs_to_produce))
    print(num_first_producers)
    for i in range(random.randint(1, num_first_producers)):
        objs.add(producer.remote())
        num_objs_to_consume += 1
        num_objs_to_produce -= 1

    time_slept = 0

    while num_objs_to_consume > 0 or num_objs_to_produce > 0:
        if random.randrange(2) and num_objs_to_produce>0:
            n = random.randint(1, num_objs_to_produce)
            for i in range(n):
                objs.add(producer.remote())
            num_objs_to_produce -= n
            num_objs_to_consume += n
        else:
            if num_objs_to_consume > 0:
                t = random.uniform(0.1, LATENCY*2)
                time.sleep(t)
                time_slept += t

                n = random.randint(1, num_objs_to_consume)
                for i in range(n):
                    res.append(consumer.remote(objs.pop()))
                num_objs_to_consume -= n
    ray.get(res)
    print("Time Slept", time_slept)

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin


ray_time = []
for i in range(NUM_TRIAL):
    ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)
    warmup(OBJECT_STORE_SIZE)

    ray_time.append(ray_pipeline_random())
    print(ray_time)
    ray.shutdown()

data = [np.std(ray_time), np.var(ray_time), WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)

print(ray_time)
#print(f"Ray Pipieline time: {sum(ray_time)/NUM_TRIAL}")
print(colored(sum(ray_time)/NUM_TRIAL,'green'))
