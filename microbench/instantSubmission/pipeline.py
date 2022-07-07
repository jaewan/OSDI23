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
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=100_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=1)
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=60)
parser.add_argument('--LATENCY', '-l', type=float, default=1)
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


def ray_pipeline():
    @ray.remote(num_cpus=1)
    def last_consumer(obj_ref):
        #time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return np.zeros(OBJECT_SIZE // 8)

    @ray.remote(num_cpus=1) 
    def producer(): 
        '''
        start_time = perf_counter()
        ret_obj = np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
        end_time = perf_counter()

        time_to_sleep = LATENCY - (end_time - start_time)
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)
        return ret_obj
        '''
        return np.zeros(OBJECT_SIZE // 8)
        
    ray_pipeline_begin = perf_counter()

    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)//NUM_STAGES
    refs = [[] for _ in range(NUM_STAGES)]
    for _ in range(WORKING_SET_RATIO*num_fill_object_store):
        refs[0].append(producer.remote())

    for stage in range(1, NUM_STAGES):
        for i in range(WORKING_SET_RATIO*num_fill_object_store):
            refs[stage].append(consumer.remote(refs[stage-1][i]))
        del refs[stage-1]

    res = []
    for i in range(WORKING_SET_RATIO*num_fill_object_store):
        res.append(last_consumer.remote(refs[NUM_STAGES-1][i]))
        #del r

    del refs[0]
    ray.get(res)

    ray_pipeline_end = perf_counter()

    del refs

    return ray_pipeline_end - ray_pipeline_begin

def baseline_pipeline():
    @ray.remote(num_cpus=1)
    def last_consumer(obj_ref):
        time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return np.zeros(OBJECT_SIZE // 8)

    @ray.remote(num_cpus=1) 
    def producer(): 
        start_time = perf_counter()
        ret_obj = ray.put(np.random.randint(2147483647, size=(OBJECT_SIZE // 8)))
        end_time = perf_counter()
        time_to_sleep = LATENCY - (end_time - start_time)
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)
        return ret_obj
        
    baseline_start = perf_counter()

    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)//NUM_STAGES
    for i in range(WORKING_SET_RATIO):
        refs = [[] for _ in range(NUM_STAGES+1)]
        for _ in range(num_fill_object_store):
            refs[0].append(producer.remote())

        for stage in range(1, NUM_STAGES):
            for r in refs[stage-1]:
                refs[stage].append(consumer.remote(r))
                #del r

        res = []
        for r in refs[NUM_STAGES-1]:
            res.append(last_consumer.remote(r))
            #del r

        del refs[0]
        ray.get(res)

    baseline_end = perf_counter()
    return baseline_end - baseline_start


ray_time = []
for i in range(NUM_TRIAL):
    ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)
    warmup()

    ray_time.append(ray_pipeline())
    print(ray_time)
    ray.shutdown()

data = [np.std(ray_time), np.var(ray_time), WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)

print(ray_time)
#print(f"Ray Pipieline time: {sum(ray_time)/NUM_TRIAL}")
print(colored(sum(ray_time)/NUM_TRIAL,'green'))
