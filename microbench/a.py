import ray
import csv
import argparse
import numpy as np 
import time
import random
import multiprocessing
from time import perf_counter

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=10_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=1)
parser.add_argument('--SEED', '-s', type=int, default=0)
parser.add_argument('--LATENCY', '-l', type=float, default=1.0)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_TRIAL = params['NUM_TRIAL']
SEED = params['SEED']
LATENCY = params['LATENCY']
OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def warmup():
    @ray.remote(num_cpus=1)
    def producer():
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*multiprocessing.cpu_count())))

    ref = []
    for i in range(multiprocessing.cpu_count()):
        ref.append(producer.remote())
    ray.get(ref[-1])
    del ref



def test():

    @ray.remote(num_cpus=1) 
    def sleep_task(): 
        time.sleep(1)
        ret = ray.put(np.random.randint(2147483647, size=(OBJECT_SIZE//(8))))
        return ret
        
    res = []
    n = multiprocessing.cpu_count()
    print("# of processes: ",n)
    
    start = perf_counter()

    n = 1
    for _ in range(n):
        res.append(sleep_task.remote())

    ray.get(res)

    end = perf_counter()

    return end - start


ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE )
warmup()

time = test()
print(time)
