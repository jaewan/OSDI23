import ray
import os
import json
import multiprocessing
import numpy as np 
from time import perf_counter

OBJECT_STORE_SIZE = 30_000_000_000
NUM_OBJS = 10

@ray.remote
def calculation_intensive():
    a = 0
    for i in range(100000000):
        if i%2 == 0:
            a += 1
        else:
            a -= 1
    return True

num_vcpu = multiprocessing.cpu_count()
physical_cores = num_vcpu//2

spill_dir = os.getenv('RAY_SPILL_DIR')
if spill_dir:
    ray.init(_system_config={"object_spilling_config": json.dumps({"type": "filesystem",
                                "params": {"directory_path": spill_dir}},)},
             num_cpus = physical_cores//2, object_store_memory=OBJECT_STORE_SIZE + 10_000_000)

objs = []
for i in range(NUM_OBJS):
    objs.append(ray.put(np.zeros(OBJECT_STORE_SIZE // (9*NUM_OBJS))))

start_time = perf_counter()
res = []
for _ in range(physical_cores):
    res.append(calculation_intensive.remote())
for r in res:
    ray.get(r)
end_time = perf_counter()

print(end_time - start_time)

