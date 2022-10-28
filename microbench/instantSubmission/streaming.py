import ray
import numpy as np
import time
from time import perf_counter
from common import *

'''
O -> O
  \
O -> O
  \
O -> O
  \
O -> O
'''

params = get_params()
OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_STAGES = params['NUM_STAGES']
NUM_TRIAL = params['NUM_TRIAL']
NUM_WORKER = params['NUM_WORKER']
LATENCY = params['LATENCY']
OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def streaming():

    @ray.remote(num_cpus=1)
    def first_consumer(obj_ref):
        #time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref, obj_ref1):
        return True

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
    n = WORKING_SET_RATIO*num_fill_object_store

    refs = [[] for _ in range(NUM_STAGES)]
    for _ in range(n):
        refs[0].append(producer.remote())

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

def offline_streaming():

    @ray.remote(num_cpus=1)
    def first_consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1)
    def consumer(obj_ref, obj_ref1):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.zeros(OBJECT_SIZE // 8)
        
    ray_pipeline_begin = perf_counter()

    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)//NUM_STAGES
    n = WORKING_SET_RATIO*num_fill_object_store

    refs = [[] for _ in range(NUM_STAGES)]
    for _ in range(n):
        refs[0].append(producer.remote())

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


####################### Test ####################### 
if TEST_OFFLINE:
    run_test(offline_pipeline)
else:
    run_test(pipeline)
