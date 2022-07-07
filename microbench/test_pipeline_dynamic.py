import ray
import csv
import argparse
import numpy as np 
import time
import random
from time import perf_counter
from termcolor import colored

####################
## Argument Parse ##
####################
parser = argparse.ArgumentParser()
parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=2)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=64_000_000_000)
parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=1_280_000_000)
parser.add_argument('--RESULT_PATH', '-r', type=str, default="../data/dummy.csv")
parser.add_argument('--NUM_TRIAL', '-t', type=int, default=5)
parser.add_argument('--NUM_WORKER', '-nw', type=int, default=32)
parser.add_argument('--SEED', '-s', type=int, default=0)
parser.add_argument('--LATENCY', '-l', type=float, default=1.65)
args = parser.parse_args()
params = vars(args)

OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
OBJECT_SIZE = params['OBJECT_SIZE'] 
WORKING_SET_RATIO = params['WORKING_SET_RATIO']
RESULT_PATH = params['RESULT_PATH']
NUM_TRIAL = params['NUM_TRIAL']
NUM_WORKER = params['NUM_WORKER']
SEED = params['SEED']
LATENCY = params['LATENCY']
OBJECT_STORE_BUFFER_SIZE = 5_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

def warmup():
    @ray.remote(num_cpus=1)
    def producer():
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*NUM_WORKER)))

    ref = []
    for i in range(NUM_WORKER+1):
        ref.append(producer.remote())
    ray.get(ref[-1])
    del ref


def test_interval_once():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        t = perf_counter()
        print(colored(t,'cyan'))
        time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        start_time = perf_counter()
        print(start_time)
        ret_obj = ray.put(np.random.randint(2147483647, size=(OBJECT_SIZE // 8)))
        end_time = perf_counter()
        time_to_sleep = LATENCY - (end_time - start_time)
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)
        return ret_obj
        
    num_fill_object_store = (OBJECT_STORE_SIZE//OBJECT_SIZE)
    n = num_fill_object_store * WORKING_SET_RATIO
    objs = []
    res = []

    ray_pipeline_begin = perf_counter()

    for _ in range(n):
        objs.append(producer.remote())

    time.sleep(LATENCY*2)

    for i in range(n):
        res.append(consumer.remote(objs[i]))

    del objs
    ray.get(res)

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

def test_interval_between_batches():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        time.sleep(1)
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        #time.sleep(0.1)
        return np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
        
    n = (OBJECT_STORE_SIZE//OBJECT_SIZE)

    ray_pipeline_begin = perf_counter()
    if(WORKING_SET_RATIO%2):
        print('Working set ratio must be even')
        quit()
    res = []

    for _ in range(WORKING_SET_RATIO//2):
        objs = []
        for i in range(n):
            objs.append(producer.remote(i))
            objs.append(producer.remote(i))

        time.sleep(LATENCY)

        for i in range(n*2):
            res.append(consumer.remote(objs[i]))

        del objs

    ray.get(res)
    del res

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

def test_ray_pipeline_random():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        time.sleep(LATENCY)
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        start_time = perf_counter()
        ret_obj = ray.put(np.random.randint(2147483647, size=(OBJECT_SIZE // 8)))
        end_time = perf_counter()
        time_to_sleep = LATENCY - (end_time - start_time)
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)
        return ret_obj
        
    random.seed(SEED)
    num_objs_to_produce = (WORKING_SET_RATIO*OBJECT_STORE_SIZE)//OBJECT_SIZE
    num_objs_to_consume = 0
    
    objs = set()
    res = []
    objs_not_consumed = 0
    idx = 0

    ray_pipeline_begin = perf_counter()

    num_first_producers = num_objs_to_produce//(random.randint(1,num_objs_to_produce))
    print(num_first_producers)
    for i in range(random.randint(1, num_first_producers)):
        objs.add(producer.remote())
        num_objs_to_consume += 1
        num_objs_to_produce -= 1

    time_slept = 0

    while num_objs_to_consume > 0 or num_objs_to_produce > 0:
        if random.randrange(2) and num_objs_to_produce>0:
            n = random.randint(1, num_objs_to_produce)
            for i in range(n):
                objs.add(producer.remote())
            num_objs_to_produce -= n
            num_objs_to_consume += n
        else:
            if num_objs_to_consume > 0:
                t = random.uniform(0.1, LATENCY*2)
                time.sleep(t)
                time_slept += t

                n = random.randint(1, num_objs_to_consume)
                for i in range(n):
                    res.append(consumer.remote(objs.pop()))
                num_objs_to_consume -= n
    ray.get(res)
    print("Time Slept", time_slept)

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin

def test_ray_pipeline_dynamic():
    @ray.remote(num_cpus=1)
    def consumer(obj_ref):
        time.sleep(1)
        return True

    @ray.remote(num_cpus=1) 
    def producer(): 
        #time.sleep(0.1)
        return np.random.randint(2147483647, size=(OBJECT_SIZE // 8))
        
    ray_pipeline_begin = perf_counter()
    num_objs_to_produce = ((OBJECT_STORE_SIZE)//OBJECT_SIZE)
    objs = []
    res = []
    idx = 0
    for i in range(num_objs_to_produce):
        objs.append(producer.remote())
        objs.append(producer.remote())
        idx += 2
    time.sleep(0.4)
    for i in range(idx):
        res.append(consumer.remote(objs[i]))
    idx = 0
    for i in range(num_objs_to_produce):
        objs.append(producer.remote())
        objs.append(producer.remote())
        idx += 2
    time.sleep(0.4)
    for i in range(idx):
        res.append(consumer.remote(objs[i]))

    del objs
    ray.get(res)

    ray_pipeline_end = perf_counter()

    return ray_pipeline_end - ray_pipeline_begin


def inspect_obj_creation_time():
    create_start_time = perf_counter()
    np.random.randint(2147483647, size=(OBJECT_SIZE//8))
    create_time = perf_counter() - create_start_time
    return create_time

ray.init(object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE , num_cpus = NUM_WORKER)

#Warm up tasks
warmup()
print('warm up finished. Test begin')

one_interval_time = []
multiple_interval_time = []
random_time = []


for i in range(NUM_TRIAL):
    print('one interval start')
    one_interval_time.append(test_interval_once())
    print(one_interval_time[i])
    '''
    print('multiple interval start')
    multiple_interval_time.append(test_interval_between_batches())
    print('random start')
    random_time.append(test_ray_pipeline_random())
    print(random_time[i])
    '''

'''
#header = ['base_std','ray_std','base_var','ray_var','working_set_ratio', 'num_stages', 'object_store_size','object_size','baseline_pipeline','ray_pipeline']
data = [np.std(base_time), np.std(ray_time), np.var(base_time), np.var(ray_time), WORKING_SET_RATIO, NUM_STAGES, OBJECT_STORE_SIZE, OBJECT_SIZE, sum(base_time)/NUM_TRIAL, sum(ray_time)/NUM_TRIAL]
with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(data)

print(f"Multiply Interval time: {sum(multiple_interval_time)/NUM_TRIAL}")
print(f"Random time: {sum(random_time)/NUM_TRIAL}")
'''
print(f"One Interval time: {sum(one_interval_time)/NUM_TRIAL}")
print(one_interval_time)
