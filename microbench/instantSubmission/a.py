import ray
import os

OBJECT_STORE_SIZE = 4_000_000_000
NUM_WORKER = 4

def basic_setup(eager_spill="true"):
    os.environ["RAY_BACKEND_LOG_LEVEL"] = "debug"
    os.environ["RAY_object_spilling_threshold"] = "1.0"
    os.environ["RAY_enable_BlockTasks"] = "false"
    os.environ["RAY_enable_BlockTasksSpill"] = "false"
    os.environ["RAY_enable_Deadlock1"] = "false"
    os.environ["RAY_enable_Deadlock2"] = "false"
    os.environ["RAY_enable_EagerSpill"] = eager_spill
    os.environ["RAY_spill_wait_time"] = "1000000"

    ray.init(object_store_memory=(OBJECT_STORE_SIZE+10_000_000), num_cpus = NUM_WORKER)

@ray.remote(num_cpus=1)
def child(arg):
    return True

@ray.remote(num_cpus=1)
def parent():
    arg = ray.put(True)
    a = child.remote(arg)
    a = ray.get(a)
    return a

basic_setup("false")

res = parent.remote()
ray.get(res)
print("Finished Running")
