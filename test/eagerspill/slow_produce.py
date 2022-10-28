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
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=100_000_000)
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=32)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
NUM_WORKER = params['NUM_WORKER']

def warmup():
    @ray.remote(num_cpus=1)
    def producer():
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*NUM_WORKER)))

    ref = []
    for i in range(NUM_WORKER-1):
        ref.append(producer.remote())
    ray.get(ref[-1])
    time.sleep(0.5)
    del ref

def basic_setup(eager_spill="true"):
    #os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"
    os.environ["RAY_object_spilling_threshold"] = "1.0"
    os.environ["RAY_enable_BlockTasks"] = "false"
    os.environ["RAY_enable_BlockTasksSpill"] = "false"
    os.environ["RAY_enable_Deadlock1"] = "false"
    os.environ["RAY_enable_Deadlock2"] = "false"
    os.environ["RAY_enable_EagerSpill"] = eager_spill
    os.environ["RAY_spill_wait_time"] = "1000000"

    ray.init(object_store_memory=(OBJECT_STORE_SIZE+10_000_000), num_cpus = NUM_WORKER)
    warmup()

def consuming_deleted():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.zeros(OBJECT_STORE_SIZE // 12)
     
    begin = perf_counter()

    objs = []
    ret = []
    for i in range(1):
        objs.append(producer.remote())
    print(colored("Submitted producers to fill the obj store",'green'))
    time.sleep(3)
    print(colored("3 sec sleep finished, eager spilled",'green'))
    for i in range(1):
        objs.append(producer.remote())
    time.sleep(3)
    ray.get(consumer.remote(objs[-1]))
    del objs[-1]
    print(colored("Deleted eager spilled objects and consumed objects in memory",'green'))
    ret = []
    for i in range(1):
        ret.append(consumer.remote(objs[i]))
    ray.get(ret[0])


    end = perf_counter()

    return end - begin

def consuming_eager_spilled():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.zeros(OBJECT_STORE_SIZE // 16)
     
    begin = perf_counter()

    objs = []
    ret = []
    for i in range(1):
        objs.append(producer.remote())
    print(colored("Submitted producers to fill the obj store",'green'))
    time.sleep(3)
    print(colored("3 sec sleep finished, eager spilled",'green'))
    for i in range(1):
        ret.append(consumer.remote(objs[i]))
    del objs
    ray.get(ret[0])
    #ray.get(ret[1])
    del ret
    time.sleep(3)
    '''
    print(colored("Consumed eager spilled objects",'green'))
    ret = []
    for i in range(2):
        ret.append(consumer.remote(producer.remote()))
    ray.get(ret[0])
    ray.get(ret[1])
    '''


    end = perf_counter()

    return end - begin

def blocking_pipeline():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        return np.zeros(OBJECT_STORE_SIZE // 16)
        
    begin = perf_counter()

    objs = []
    ret = []
    for i in range(2):
        objs.append(producer.remote())
    print(colored("Submitted producers to fill the obj store",'green'))
    time.sleep(3)
    objs.append(producer.remote())
    print(colored("3 sec sleep finished, OOM task submitted",'green'))
    ret.append(consumer.remote(objs[-1]))
    del objs[-1]
    print(colored("consumer task submitted",'green'))
    ray.get(ret[0])
    print(colored("consumer task returned",'green'))
    for i in range(2):
        ret.append(consumer.remote(objs[i]))
    del objs
    ray.get(ret)
    del ret

    end = perf_counter()

    return end - begin

def shuffle():
    @ray.remote
    def map(npartitions):
        object_size = (OBJECT_STORE_SIZE//(npartitions//2))
        data = np.random.rand(object_size//8)
        size = len(data)//npartitions

        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def reduce(*partitions):
        #time.sleep(1)
        return True

    shuffle_start = perf_counter()

    npartitions = 20
    refs = [map.options(num_returns=npartitions).remote(npartitions)
            for _ in range(npartitions)]

    results = []
    for j in range(npartitions):
        results.append(reduce.remote(*[ref[j] for ref in refs]))
    del refs
    ray.get(results)
    del results

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

NPARTITIONS = 2

def multi_stage_shuffle():
    @ray.remote
    def map(npartitions):
        object_size = (OBJECT_STORE_SIZE//(npartitions//WORKING_SET_RATIO))
        data = np.random.rand(object_size//8)
        size = len(data)//npartitions

        return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))

    @ray.remote
    def staged_map(*partitions):
        object_size = (OBJECT_STORE_SIZE//(NPARTITIONS//WORKING_SET_RATIO))
        data = np.random.rand(object_size//8)
        size = len(data)//NPARTITIONS

        return tuple(data[(i*size):((i+1)*size)] for i in range(NPARTITIONS))

    @ray.remote
    def reduce(*partitions):
        #time.sleep(1)
        return True

    shuffle_start = perf_counter()

    npartitions = NPARTITIONS
    refs = [map.options(num_returns=npartitions).remote(npartitions)
            for _ in range(npartitions)]
    refs1 = []
    for j in range(npartitions):
        refs1.append(staged_map.options(num_returns=npartitions).remote(*[ref[j] for ref in refs]))
    del refs

    results = []
    for j in range(npartitions):
        results.append(reduce.remote(*[ref[j] for ref in refs1]))
    del refs1

    ray.get(results)
    del results

    shuffle_end = perf_counter()
    return shuffle_end - shuffle_start

def pipeline():
    @ray.remote(num_cpus=1)
    def last_consumer(obj_ref):
        #time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        object_size = (OBJECT_STORE_SIZE//(16))
        return np.zeros(object_size)
        
    ray_pipeline_begin = perf_counter()

    n = 1

    obj = []
    for i in range(n):
        obj.append(producer.remote())

    res = []
    for i in range(n):
        res.append(last_consumer.remote(obj[i]))
    del obj

    for i in range(n):
        ray.get(res[i])
    del res

    ray_pipeline_end = perf_counter()


    return ray_pipeline_end - ray_pipeline_begin

NUM_STAGES = 2

def multi_stage_pipeline():
    @ray.remote(num_cpus=1)
    def last_consumer(obj_ref):
        #time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1)
    def interim(obj_ref):
        object_size = (OBJECT_STORE_SIZE//(16))
        return np.zeros(object_size)

    @ray.remote(num_cpus=1) 
    def producer(): 
        object_size = (OBJECT_STORE_SIZE//(16))
        return np.zeros(object_size)
        
    ray_pipeline_begin = perf_counter()

    n = 2

    obj = []
    for i in range(n):
        obj.append(producer.remote())

    obj1 = []
    for i in range(n):
        obj1.append(interim.remote(obj[i]))
    del obj

    res = []
    for i in range(n):
        res.append(last_consumer.remote(obj1[i]))
    del obj1

    for i in range(n):
        ray.get(res[i])
    del res

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

def measure_eager_spill_overhead():
    @ray.remote(num_cpus=1)
    def last_consumer(obj_ref):
        #time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1) 
    def compute_heavy(): 
        a = 0
        for i in range(50000000):
            if i%2 == 0:
                a += 1
            else:
                a -=1
        return True


    @ray.remote(num_cpus=1) 
    def fill_object_store(): 
        object_size = (OBJECT_STORE_SIZE//(8))
        return np.zeros(object_size)
        
    ray_pipeline_begin = perf_counter()

    obj = fill_object_store.remote()

    compute_res = []
    for i in range(NUM_WORKER//2):
        compute_res.append(compute_heavy.remote())
    for i in range(NUM_WORKER//2):
        ray.get(compute_res[i])
    del compute_res
    res = last_consumer.remote(obj)
    del obj
    ray.get(res)

    ray_pipeline_end = perf_counter()


    return ray_pipeline_end - ray_pipeline_begin


f = measure_eager_spill_overhead
basic_setup("false")
print("DFS", f())
print("consuming_deleted", consuming_deleted())
print("consuming_eager_spilled", consuming_eager_spilled())
print("blocking_pipeline", blocking_pipeline())
print("shuffle", shuffle())
print("multi_stage_shuffle", shuffle())
'''
'''
ray.shutdown()
basic_setup()
print("eager spill version", f())
