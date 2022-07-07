import ray
import csv
import argparse
import numpy as np 
import time
import multiprocessing
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=1_000_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=5)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_STAGES = params['NUM_STAGES']
NUM_TRIAL = params['NUM_TRIAL']
OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def test():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        #time.sleep(0.1)
        return np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
        
    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)
    n = num_fill_object_store * WORKING_SET_RATIO
    objs = []
    res = []

    ray_pipeline_begin = perf_counter()

    objs.append(producer.remote())
    res.append(consumer.remote(objs[0]))

    time.sleep(5)

    res.append(consumer.remote(objs[0]))

    ray.get(res)
    del objs

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin



ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE )

ray_time = []
for i in range(NUM_TRIAL):
    ray_time.append(test())

print(f"Ray Pipieline time: {sum(ray_time)/NUM_TRIAL}")
