import ray
import os
import argparse
import numpy as np
import time
from time import perf_counter
from termcolor import colored

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=8)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=100_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/shuffle.csv")
parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=1)
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=60)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_STAGES = params['NUM_STAGES']
NUM_TRIAL = params['NUM_TRIAL']
NUM_WORKER = params['NUM_WORKER']
OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def basic_setup():
    os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"
    os.environ["RAY_object_spilling_threshold"] = "1.0"
    os.environ["RAY_enable_BlockTasks"] = "false"
    os.environ["RAY_enable_BlockTasksSpill"] = "true"
    os.environ["RAY_enable_Deadlock1"] = "false"
    os.environ["RAY_enable_Deadlock2"] = "true"
    os.environ["RAY_spill_wait_time"] = "3000"

    ray.init(object_store_memory=OBJECT_STORE_SIZE, num_cpus = NUM_WORKER)

def scatter():
    @ray.remote
    def map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        return True

    start = perf_counter()

    npartitions = (OBJECT_STORE_SIZE*WORKING_SET_RATIO)//OBJECT_SIZE 
    refs = [map.options(num_returns=npartitions).remote(npartitions)
            for _ in range(npartitions)]

    results = reduce.remote(*[ref[0] for ref in refs])
    del refs
    ray.get(results)
    del results

    end = perf_counter()
    return end - start


ray_time = []
basic_setup()
for i in range(NUM_TRIAL):
    ray_time.append(scatter())

print(colored(sum(ray_time)/NUM_TRIAL,'green'))

#ray.timeline("timeline.json")
