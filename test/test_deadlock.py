##########################################################
#       Pytest Deadlock Test
#       Deadlock #1. When all workers are spinning (This is no more necessary)
#       Deadlock #2. Entangled Dependencies
#           1. Shuffle
#           2. Scatter-Gather
#           3. Streaming
##########################################################
import pytest
import os
import ray
import numpy as np 
import time

####################
## Arguments ##
####################
OBJECT_STORE_SIZE = 4_000_000_000
NUM_WORKER = 30
OBJECT_STORE_BUFFER_SIZE = 1_000_000
TIME_OUT_VAL = 60

def basic_setup():
    os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"
    os.environ["RAY_object_spilling_threshold"] = "1.0"
    os.environ["RAY_enable_BlockTasks"] = "false"
    os.environ["RAY_enable_BlockTasksSpill"] = "true"
    os.environ["RAY_enable_Deadlock1"] = "false"
    os.environ["RAY_enable_Deadlock2"] = "true"
    ray.init(object_store_memory=OBJECT_STORE_SIZE, num_cpus = NUM_WORKER)

@pytest.mark.timeout(timeout=TIME_OUT_VAL, method="thread")
def test_simple():
    basic_setup()

    @ray.remote(num_cpus=1)
    def consumer(a, b, c):
        return True

    @ray.remote(num_cpus=1) 
    def small_obj_producer(): 
        obj_size = (OBJECT_STORE_SIZE)//64
        return np.zeros(obj_size)

    @ray.remote(num_cpus=1) 
    def large_obj_producer(): 
        obj_size = (OBJECT_STORE_SIZE + 100_000_000)//16
        return np.zeros(obj_size)

    small_obj = small_obj_producer.remote()
    time.sleep(1.5)

    large_obj1 = large_obj_producer.remote()
    large_obj2 = large_obj_producer.remote()
    res = consumer.remote(large_obj1, large_obj2, small_obj)

    ray.get(res)

    ray.shutdown()

    assert True

@pytest.mark.timeout(timeout=TIME_OUT_VAL, method="thread")
def test_all_workers_spinning():
    os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"
    os.environ["RAY_object_spilling_threshold"] = "1.0"
    os.environ["RAY_enable_BlockTasks"] = "false"
    os.environ["RAY_enable_BlockTasksSpill"] = "true"
    os.environ["RAY_enable_Deadlock1"] = "true"
    os.environ["RAY_enable_Deadlock2"] = "false"
    ray.init(object_store_memory=OBJECT_STORE_SIZE, num_cpus = NUM_WORKER)

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def fill_object_store(): 
        obj_size = (OBJECT_STORE_SIZE)//8
        return np.zeros(obj_size)

    @ray.remote(num_cpus=1) 
    def producer(): 
        obj_size = (OBJECT_STORE_SIZE//NUM_WORKER)//8
        print(obj_size)
        return np.zeros(obj_size)

    #Fill the object store
    filling_obj = fill_object_store.remote()
    time.sleep(3)

    # Submit producers so all workers are spinning
    objs = []
    for _ in range(NUM_WORKER):
        objs.append(producer.remote())

    res = consumer.remote(objs)
    print("Calling filling_obj consumer")
    r = consumer.remote(filling_obj)
    ray.get(res)
    ray.get(r)
    print("Called filling_obj consumer")

    del objs

    ray.shutdown()

    assert True

@pytest.mark.timeout(timeout=TIME_OUT_VAL, method="thread")
def test_gather():
    basic_setup()

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        obj_size = (OBJECT_STORE_SIZE//NUM_WORKER)//8
        obj_size += (OBJECT_STORE_BUFFER_SIZE)
        return np.zeros(obj_size)

    #Produces over object_store size 
    objs = []
    for _ in range(NUM_WORKER):
        objs.append(producer.remote())

    res = consumer.remote(objs)
    ray.get(res)

    ray.shutdown()

    assert True

#scatter is not detected in current version
#Ray produces a single object no matter how many object it returns
@pytest.mark.timeout(timeout=TIME_OUT_VAL, method="thread")
def scatter_gather():
    basic_setup()

    @ray.remote
    def scatter(object_store_size):
        obj_size = (OBJECT_STORE_SIZE//NUM_WORKER)//8
        obj_size += (OBJECT_STORE_BUFFER_SIZE)
        return tuple(np.zeros(obj_size) for i in range(NUM_WORKER))

    @ray.remote
    def worker(partitions):
        return True

    @ray.remote
    def gather(*avgs):
        return True


    scatter_outputs = scatter.options(num_returns=NUM_WORKER).remote(OBJECT_STORE_SIZE)
    outputs = []
    for j in range(NUM_WORKER):
        outputs.append(worker.remote(scatter_outputs[j]))
    del scatter_outputs
    gather_outputs = []
    gather_outputs.append(gather.remote(*[o for o in outputs]))
    del outputs
    ray.get(gather_outputs)

    del gather_outputs
    ray.shutdown()

    assert True

@pytest.mark.timeout(timeout=TIME_OUT_VAL, method="thread")
def test_shuffle():
    basic_setup()

    @ray.remote
    def map(object_size):
        size = object_size//NUM_WORKER
        data = np.zeros(object_size)
        return tuple(data[(i*size):((i+1)*size)] for i in range(NUM_WORKER))

    @ray.remote
    def reduce(*partitions):
        return True

    #Set object size to overcommit the object store
    object_size = OBJECT_STORE_SIZE//(NUM_WORKER*8)
    object_size += (OBJECT_STORE_BUFFER_SIZE //8)

    #Map
    refs = [map.options(num_returns=NUM_WORKER).remote(object_size)
            for _ in range(NUM_WORKER)]

    results = []
    for j in range(NUM_WORKER):
        results.append(reduce.remote(*[ref[j] for ref in refs]))
    del refs
    ray.get(results)
    del results

    return True

@pytest.mark.timeout(timeout=TIME_OUT_VAL, method="thread")
def test_streaming():
    basic_setup()

    @ray.remote
    def producer():
        return

    @ray.remote
    def consumer(obj1, obj2):
        return True

    @ray.remote
    def single_obj_consumer(obj1):
        return True

    objs = []
    res = []
    for i in range(4):
        objs.append(producer.options(num_returns=2).remote())
    res.append(single_obj_consumer.remote(objs[0][0]))
    for i in range(1, 4-1):
        res.append(consumer.remote(objs[i][0], objs[i][1]))

    del objs
    ray.get(res)

    ray.shutdown()

    assert True
