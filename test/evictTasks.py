##########################################################
#       Test Deadlock caused by backpressure
# Backpressure sets a limit priority if it reaches a certain threshold
# There was a deadlock when tasks were submitted dynamically
##########################################################
import pytest
import ray
import os
import time
import numpy as np 

####################
## Arguments ##
####################
OBJECT_STORE_SIZE = 4_000_000_000
NUM_WORKER = 4
OBJECT_STORE_BUFFER_SIZE = 1_000_000
RAY_block_tasks_threshold = 1.0

def basic_setup():
    os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"
    os.environ["RAY_object_spilling_threshold"] = "100.0"
    os.environ["RAY_block_tasks_threshold"] = "1.0"
    os.environ["RAY_enable_Deadlock2"] = "true"
    os.environ["RAY_enable_BlockTasksSpill"] = "true"
    os.environ["RAY_enable_EvictTasks"] = "true"
    os.environ["RAY_spill_wait_time"] = "100000"
    ray.init(object_store_memory=OBJECT_STORE_SIZE, num_cpus = NUM_WORKER)


def test_backpressure_deadlock():
    basic_setup()

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def producer(size): 
        return np.zeros(size)

    objs = []
    objs.append(producer.remote(OBJECT_STORE_SIZE//8))
    time.sleep(1)

    size = OBJECT_STORE_SIZE//(8*(NUM_WORKER))
    for i in range(NUM_WORKER):
        objs.append(producer.remote(size))

    time.sleep(1)
    res = []
    for i in range(NUM_WORKER+1):
        res.append(consumer.remote(objs[i]))
    del objs
    ray.get(res[-1])

    return True

test_backpressure_deadlock()
os.system('ray memory --stats-only')
