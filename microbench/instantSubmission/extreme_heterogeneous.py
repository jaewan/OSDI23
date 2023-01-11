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

def het():
    @ray.remote
    def map(npartitions):
        size = OBJECT_SIZE//8
        data = np.random.rand(size)
        size = size//npartitions
        time.sleep(LATENCY)
        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        #time.sleep(1)
        return True

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

    start_time = perf_counter()

    working_set_size = (OBJECT_STORE_SIZE*WORKING_SET_RATIO)//3
    npartitions = (working_set_size)//OBJECT_SIZE 
    map_refs = [map.options(num_returns=npartitions).remote(npartitions)
            for _ in range(npartitions)]
    num_fill_object_store = (working_set_size//OBJECT_SIZE)//NUM_STAGES
    pipeline_map_refs = [[] for _ in range(NUM_STAGES)]
    for _ in range(num_fill_object_store):
        pipeline_map_refs[0].append(producer.remote())

    for stage in range(1, NUM_STAGES):
        for i in range(num_fill_object_store):
            pipeline_map_refs[stage].append(consumer.remote(pipeline_map_refs[stage-1][i]))
        del pipeline_map_refs[stage-1]

    streaming_refs = [[] for _ in range(NUM_STAGES)]
    for _ in range(n):
        streaming_refs[0].append(producer.remote())

    for stage in range(1, NUM_STAGES):
        for i in range(n):
            streaming_refs[stage].append(consumer.remote(streaming_refs[stage-1][i]))
        del streaming_refs[stage-1]

    shuffle_refs = []
    for j in range(npartitions):
        shuffle_refs.append(reduce.remote(*[ref[j] for ref in map_refs]))
    del map_refs

    pipeline_res = []
    for i in range(num_fill_object_store):
        pipeline_res.append(last_consumer.remote(pipeline_map_refs[NUM_STAGES-1][i]))
        #del r
    del pipeline_map_refs

    streaming_res = []
    streaming_res.append(first_consumer.remote(streaming_refs[NUM_STAGES-1][0]))
    for i in range(1, n):
        streaming_res.append(consumer.remote(streaming_refs[NUM_STAGES-1][i-1], streaming_refs[NUM_STAGES-1][i-1]))
    del streaming_refs

    for r in shuffle_res:
        ray.get(r)
    del shuffle_res
    
    for r in pipeline_res:
        ray.get(r)
    del pipeline_res
    
    for r in streaming_res:
        ray.get(r)
    del streaming_res

    end_time = perf_counter()


if __name__ == '__main__':
    run_test(het)
