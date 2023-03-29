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

def pipeline():
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
        time.sleep(LATENCY)
        return np.zeros(OBJECT_SIZE // 8)

        
    ray_pipeline_begin = perf_counter()

    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)//NUM_STAGES

    print("Object store size",OBJECT_STORE_SIZE)
    print("Object size",OBJECT_SIZE)
    print("Working set ratio is", WORKING_SET_RATIO)
    print("Submitting ", WORKING_SET_RATIO*num_fill_object_store)

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
    del refs

    ray.get(res[-1])

    ray_pipeline_end = perf_counter()


    return ray_pipeline_end - ray_pipeline_begin

def offline_pipeline():
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
        for r in res:
            ray.get(r)
        #ray.get(res)

    baseline_end = perf_counter()
    return baseline_end - baseline_start

####################### Test ####################### 
if __name__ == '__main__':
    if TEST_OFFLINE:
        print('Off version')
        run_test(offline_pipeline)
    else:
        print('Online version')
        run_test(pipeline)
