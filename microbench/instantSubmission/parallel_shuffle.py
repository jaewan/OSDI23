import ray
import numpy as np
import time
from time import perf_counter
from common import *

params = get_params()

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_TRIAL = params['NUM_TRIAL']
TEST_OFFLINE = params['OFFLINE']
OBJECT_STORE_BUFFER_SIZE = 5_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def shuffle():
    @ray.remote
    def map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        time.sleep(1)
        return True

    shuffle_start = perf_counter()

    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    refs = []
    for i in range(WORKING_SET_RATIO):
        refs.append([map.options(num_returns=npartitions).remote(npartitions)
            for _ in range(npartitions)])
    results = []
    for i in range(WORKING_SET_RATIO):
        for j in range(npartitions):
            results.append(reduce.remote(*[ref[j] for ref in refs[i]]))
    del refs
    ray.get(results)
    del results

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

def offline_shuffle():
    @ray.remote
    def map(npartitions):
        data = np.random.rand(OBJECT_SIZE // 8)
        size = OBJECT_SIZE//npartitions
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        time.sleep(1)
        return True

    shuffle_start = perf_counter()

    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    for _ in range(WORKING_SET_RATIO):
        map_outputs = [
                map.options(num_returns=npartitions).remote(npartitions)
                for _ in range(npartitions)]
        outputs = []
        for i in range(npartitions):
            outputs.append(reduce.remote(*[partition[i] for partition in map_outputs]))
        del map_outputs 
        ray.get(outputs)
        del outputs

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

####################### Test ####################### 
if TEST_OFFLINE:
    run_test(offline_shuffle)
else:
    run_test(shuffle)

