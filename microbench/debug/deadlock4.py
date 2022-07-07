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
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=1_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=500_000_000)
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


def test():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def single_producer(): 
        return np.random.randint(2147483647, size=(OBJECT_SIZE // 8))

    @ray.remote(num_cpus=1) 
    def double_producer(): 
        return tuple(np.random.randint(2147483647, size=(OBJECT_SIZE // 8)) for _ in range(2))
        

    obj1 = single_producer.remote()

    time.sleep(0.5) #wait for producer to finish creating an obj

    obj2, obj3 = double_producer.options(num_returns=2).remote()

    result = consumer.remote(obj2)
    result = consumer.remote(obj3)

    ray.get(result)

    return True


ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE )

print("Test Begin")
test()
print("Test End")
