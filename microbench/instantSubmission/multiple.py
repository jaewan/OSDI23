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
NUM_STAGES = params['NUM_STAGES']
NUM_TRIAL = params['NUM_TRIAL']
NUM_WORKER = params['NUM_WORKER']
LATENCY = params['LATENCY']
TEST_OFFLINE = params['OFFLINE']
OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

@ray.remote(num_cpus=1) 
  def starter(): 
    return True

def pipeline():
    @ray.remote(num_cpus=1)
    def last_consumer(obj_ref):
        #time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return np.zeros(OBJECT_SIZE // 8)

    @ray.remote(num_cpus=1) 
    def producer(arg): 
        '''
        start_time = perf_counter()
        ret_obj = np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
        end_time = perf_counter()

        time_to_sleep = LATENCY - (end_time - start_time)
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)
        return ret_obj
        '''
        time.sleep(LATENCY)
        return np.zeros(OBJECT_SIZE // 8)
        
    ray_pipeline_begin = perf_counter()

    a = starter.remote()
    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)//NUM_STAGES
    refs = [[] for _ in range(NUM_STAGES)]
    for _ in range(WORKING_SET_RATIO*num_fill_object_store):
        refs[0].append(producer.remote(a))
    del a

    for stage in range(1, NUM_STAGES):
        for i in range(WORKING_SET_RATIO*num_fill_object_store):
            refs[stage].append(consumer.remote(refs[stage-1][i]))
        del refs[stage-1]

    res = []
    for i in range(WORKING_SET_RATIO*num_fill_object_store):
        res.append(last_consumer.remote(refs[NUM_STAGES-1][i]))
        #del r

    del refs[0]
    del refs

    ray.get(res[-1])

    ray_pipeline_end = perf_counter()


    return ray_pipeline_end - ray_pipeline_begin

def shuffle():
    @ray.remote
    def map(arg, npartitions):
        size = OBJECT_SIZE//8
        data = np.random.rand(size)
        size = size//npartitions
        time.sleep(LATENCY)
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        #time.sleep(1)
        return True

    shuffle_start = perf_counter()

    a = starter.remote()
    npartitions = (OBJECT_STORE_SIZE*WORKING_SET_RATIO)//OBJECT_SIZE 
    refs = [map.options(num_returns=npartitions).remote(a, npartitions)
            for _ in range(npartitions)]
    del a

    results = []
    for j in range(npartitions):
        results.append(reduce.remote(*[ref[j] for ref in refs]))
    del refs
    ray.get(results)
    del results

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

def streaming():

    @ray.remote(num_cpus=1)
    def first_consumer(obj_ref):
        #time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref, obj_ref1):
        return True

    @ray.remote(num_cpus=1) 
    def producer(arg): 
        return np.zeros(OBJECT_SIZE // 8)
        
    ray_pipeline_begin = perf_counter()

    a = starter.remote()
    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)//NUM_STAGES
    n = WORKING_SET_RATIO*num_fill_object_store

    refs = [[] for _ in range(NUM_STAGES)]
    for _ in range(n):
        refs[0].append(producer.remote(a))
    del a

    for stage in range(1, NUM_STAGES):
        for i in range(n):
            refs[stage].append(consumer.remote(refs[stage-1][i]))
        del refs[stage-1]

    res = []
    res.append(first_consumer.remote(refs[NUM_STAGES-1][0]))
    for i in range(1, n):
        res.append(consumer.remote(refs[NUM_STAGES-1][i-1], refs[NUM_STAGES-1][i-1]))
        #del r

    del refs[0]
    ray.get(res[-1])

    ray_pipeline_end = perf_counter()

    del refs

    return ray_pipeline_end - ray_pipeline_begin
if __name__ == '__main__':
    pipeline()
    shuffle()
    streaming()
    print("done!")
