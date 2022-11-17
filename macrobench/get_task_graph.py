CORE_WORKER_PATH = "/tmp/ray/core_worker_log"
SCHEDULER_PATH = "/tmp/ray/scheduler_log"

def get_taskName_priority(s):
    task_name = s.split()
    task_name = task_name[1]
    priority = s[s.index('['):s.index(']')+1]
    return task_name, priority

def index_containing_substring(the_list, substring):
    for i, s in enumerate(the_list):
        if substring in s:
            return get_taskName_priority(s)
    return -1, -1

'''
def parse_logs():
    import glob

    for fpath in glob.glob('/tmp/ray/session_latest/logs/python-core-driver-*'):
        with open(fpath, 'r') driver_log_file:
            for line in driver_log_file:
                if "Requesting lease"

    quit()
    #with open('/tmp/ray/session_latest/logs/raylet.out', 'r') as raylet:

import sys

is_production_ray = False
print(len(sys.argv))
if len(sys.argv) > 1:
    parse_logs()
'''

with open (CORE_WORKER_PATH, "r") as core_log_file:
    core_log = core_log_file.readlines()
    with open (SCHEDULER_PATH, "r") as driver_log:
        for line in driver_log:
            first_appearance = True
            occurence = 0
            last_function, _ = get_taskName_priority(core_log[0])
            for task_id in line.split():
                task_name, priority = index_containing_substring(core_log,task_id)
                if priority == -1:
                    continue
                if last_function == task_name:
                    occurence += 1
                else:
                    print(f"task name: {last_function}\t\t occurance:{occurence}")
                    last_function = task_name
                    occurence = 1
            print(f"task name: {last_function}\t\t\t occurance:{occurence}")
