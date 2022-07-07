import ray
import csv
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
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/scatter_gather.csv")
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=60)
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=1)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_TRIAL = params['NUM_TRIAL']
NUM_WORKER = params['NUM_WORKER']
OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def warmup():
    @ray.remote(num_cpus=1)
    def producer():
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*NUM_WORKER)))

    ref = []
    for i in range(NUM_WORKER):
        ref.append(producer.remote())
    ray.get(ref[-1])
    time.sleep(0.5)
    del ref

def test_ray_scatter_gather():
    @ray.remote
    def scatter(npartitions, object_store_size):
        size = (object_store_size // 8)//npartitions
        #return tuple(data[(i*size):((i+1)*size)] for i in range(npartitions))
        return tuple(np.random.randint(1<<31, size=size) for i in range(npartitions))

    @ray.remote
    def worker(partitions):
        time.sleep(1)
        return np.average(partitions)

    @ray.remote
    def gather(*avgs):
        return np.average(avgs)

    scatter_gather_start = perf_counter()

    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    scatter_outputs = [scatter.options(num_returns=npartitions).remote(npartitions, OBJECT_STORE_SIZE) for _ in range(WORKING_SET_RATIO)]
    outputs = [[] for _ in range(WORKING_SET_RATIO)]
    for i in range(WORKING_SET_RATIO):
        for j in range(npartitions):
            outputs[i].append(worker.remote(scatter_outputs[i][j]))
    del scatter_outputs
    gather_outputs = []
    for i in range(WORKING_SET_RATIO):
        gather_outputs.append(gather.remote(*[o for o in outputs[i]]))
    del outputs
    ray.get(gather_outputs)

    scatter_gather_end = perf_counter()
    del gather_outputs
    return scatter_gather_end - scatter_gather_start

def test_baseline_scatter_gather():
    @ray.remote
    def scatter(npartitions, object_store_size):
        size = (object_store_size // 8)//npartitions
        return tuple(np.random.randint(1<<31, size=size) for i in range(npartitions))

    @ray.remote
    def worker(partitions):
        time.sleep(1)
        return np.average(partitions)

    @ray.remote
    def gather(*avgs):
        return np.average(avgs)

    scatter_gather_start = perf_counter()

    npartitions = OBJECT_STORE_SIZE//OBJECT_SIZE 
    for _ in range(WORKING_SET_RATIO):
        scatter_outputs = scatter.options(num_returns=npartitions).remote(npartitions, OBJECT_STORE_SIZE)
        outputs = []
        for i in range(npartitions):
            outputs.append(worker.remote(scatter_outputs[i]))
        del scatter_outputs
        ray.get(gather.remote(*outputs))

    scatter_gather_end = perf_counter()
    return scatter_gather_end - scatter_gather_start


ray_time = []
for i in range(NUM_TRIAL):
    ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)
    warmup()

    ray_time.append(test_ray_scatter_gather())
    ray.shutdown()

#header = ['base_std','ray_std','base_std, 'ray_std', 'base_var','ray_var','working_set_ratio', 'object_store_size','object_size','baseline_pipeline','ray_pipeline']
data = [np.std(ray_time), np.var(ray_time), WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)

print(f"Ray scatter_gather time: {sum(ray_time)/NUM_TRIAL}")
print(colored(sum(ray_time)/NUM_TRIAL,'green'))
