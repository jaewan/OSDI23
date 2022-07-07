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
NUM_WORKER = 30
OBJECT_STORE_BUFFER_SIZE = 1_000_000
TIME_OUT_VAL = 30
RAY_block_tasks_threshold = 0.8

def basic_setup():
    os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"
    os.environ["RAY_object_spilling_threshold"] = "1.0"
    os.environ["RAY_block_tasks_threshold"] = str(RAY_block_tasks_threshold)
    os.environ["RAY_enable_BlockTasks"] = "true"
    ray.init(object_store_memory=OBJECT_STORE_SIZE, num_cpus = NUM_WORKER)


@pytest.mark.timeout(timeout=TIME_OUT_VAL, method="thread")
def test_backpressure_deadlock():
    basic_setup()

    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        return True

    @ray.remote(num_cpus=1) 
    def produce_over_threshold(): 
        size = int((OBJECT_STORE_SIZE*RAY_block_tasks_threshold)//8)
        return np.zeros(size)

    @ray.remote(num_cpus=1) 
    def produce_small_object(): 
        size = int((OBJECT_STORE_SIZE*(1-RAY_block_tasks_threshold))//16)
        return np.zeros(size)

    big_obj = produce_over_threshold.remote()
    time.sleep(1)
    small_obj = produce_small_object.remote()

    res1 = consumer.remote(big_obj)
    res2 = consumer.remote(small_obj)
    del big_obj
    del small_obj

    ray.get(res1)
    ray.get(res2)
        
    assert True

