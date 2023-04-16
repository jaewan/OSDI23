import ray
import csv
import numpy as np 
import time
import argparse
import os
import json
import ray
from termcolor import colored
import re

params=0

def boolean_string(s):
    if s not in {'False', 'True', 'false', 'true'}:
        raise ValueError('Not a valid boolean string')
    return (s == 'True' or  s == 'true')

def get_params():
    global params
    parser = argparse.ArgumentParser()
    parser.add_argument('--WORKING_SET_RATIO', '-w', type=int, default=1)
    parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=4_000_000_000)
    parser.add_argument('--OBJECT_SIZE', '-os', type=int, default=100_000_000)
    parser.add_argument('--RESULT_PATH', '-r', type=str, default="~/OSDI/data/dummy.csv")
    parser.add_argument('--NUM_TRIAL', '-t', type=int, default=1)
    parser.add_argument('--NUM_STAGES', '-ns', type=int, default=1)
    parser.add_argument('--NUM_WORKER', '-nw', type=int, default=32)
    parser.add_argument('--SEED', '-s', type=int, default=0)
    parser.add_argument('--LATENCY', '-l', type=float, default=0)
    parser.add_argument('--OFFLINE', '-off', type=boolean_string, default=False)
    parser.add_argument('--MULTI_NODE', '-m', type=boolean_string, default=False)
    args = parser.parse_args()
    params = vars(args)

    return params

def warmup(OBJECT_STORE_SIZE):
    @ray.remote(num_cpus=1)
    def producer(n):
        return np.random.randint(2147483647, size=(OBJECT_STORE_SIZE//(8*n*2)))

    @ray.remote(num_cpus=1)
    def consumer(obj):
        return True
    res = []
    n =2 
    for i in range(n):
        res.append(consumer.remote(producer.remote(n)))
    ray.get(res)
    del res
    time.sleep(1)

def get_num_spilled_objs(is_multi_node):
    os.system('ray memory --stats-only > /tmp/ray/spilllog')
    migration_count = 0
    num = 0
    size = 0
    if os.path.isfile("/tmp/ray/migration_count"):
        with open("/tmp/ray/migration_count", 'r') as file:
            for line in file:
                migration_count = int(line)
    with open("/tmp/ray/spilllog", 'r') as file:
        lines = file.readlines()
        for line in lines:
            if line.find("Spilled") != -1:
                line = line.split()
                idx = line.index('Spilled')
                num += int(line[idx+3])
                size += int(line[idx+1])
    if is_multi_node:
        import sys
        sys.path.insert(0, '/home/'+os.getlogin()+'/OSDI23/script/multinode/')
        # This function shutsdown remote ray instance
        from gather_migration_count import get_migration_count_from_remote
        num_workers, remote_migration_count = get_migration_count_from_remote()
        migration_count += remote_migration_count
        migration_count/=(num_workers + 1)
    return num,size,migration_count

def run_test(benchmark):
    OBJECT_STORE_SIZE = params['OBJECT_STORE_SIZE'] 
    OBJECT_SIZE = params['OBJECT_SIZE'] 
    WORKING_SET_RATIO = params['WORKING_SET_RATIO']
    RESULT_PATH = params['RESULT_PATH']
    NUM_TRIAL = params['NUM_TRIAL']
    NUM_WORKER = params['NUM_WORKER']
    MULTI_NODE = params['MULTI_NODE']
    OBJECT_STORE_BUFFER_SIZE = 50_000_000 #this value is to add some space in ObjS for nprand metadata and ray object metadata

    debugging = False
    ray_time = []
    num_spilled_objs = 0
    spilled_size = 0
    migration_counts = 0

    if 'dummy' in RESULT_PATH:
        debugging = True

    for i in range(NUM_TRIAL):
        if MULTI_NODE:
            ray.init()
        else:
            spill_dir = os.getenv('RAY_SPILL_DIR')
            if spill_dir:
                ray.init(_system_config={"object_spilling_config": json.dumps({"type": "filesystem",
                                        "params": {"directory_path": spill_dir}},)}, num_cpus=NUM_WORKER, object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE )
            else:
                ray.init(num_cpus=NUM_WORKER, object_store_memory=OBJECT_STORE_SIZE+OBJECT_STORE_BUFFER_SIZE )

        if not debugging:
            warmup(OBJECT_STORE_SIZE)

        ray_time.append(benchmark())
        print("Ended")
        num,size,migration_count = get_num_spilled_objs(MULTI_NODE)
        num_spilled_objs += num
        spilled_size += size
        migration_counts += migration_count

        print(ray_time, num, size, migration_count)
        if debugging:
            ray.timeline('/tmp/ray/timeline.json')
        ray.shutdown()

    if not debugging:
        data = [sum(ray_time)/NUM_TRIAL, num_spilled_objs//NUM_TRIAL, spilled_size//NUM_TRIAL, migration_counts//NUM_TRIAL,
                WORKING_SET_RATIO, OBJECT_STORE_SIZE, OBJECT_SIZE, np.std(ray_time), np.var(ray_time)]
        with open(RESULT_PATH, 'a', encoding='UTF-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(data)

    print(ray_time)
    print(colored(sum(ray_time)/NUM_TRIAL,'green'))
