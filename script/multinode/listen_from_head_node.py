import socket
import pickle
import struct
import os
import shutil
import node_info

os.environ['RAY_worker_lease_timeout_milliseconds']='0'
os.environ['RAY_object_spilling_threshold']='1.0'
os.environ['RAY_block_tasks_threshold']='1.0'
os.environ['RAY_worker_cap_enabled']='False'

PORT = node_info.PORT

def sanitize_data(data_dict):
    for key in data_dict:
        if type(data_dict[key]) is bool:
            if data_dict[key]:
                data_dict[key] = 'true'
            else:
                data_dict[key] = 'false'

def push_based_shuffle_setup(scheduling_level):
    username = os.getlogin()
    PRODUCTION_DIR = '/home/'+username+'/production_ray/python/ray/data/_internal'
    BOA_DIR = '/home/'+username+'/ray_memory_management/python/ray/data/_internal'
    shuffle_file = '/home/'+username+'/OSDI23/macrobench/code/'

    if scheduling_level == 0:
        shuffle_file += "application_scheduling/push_based_shuffle.py"
    elif scheduling_level == 1:
        shuffle_file += "application_scheduling_off_ver1/push_based_shuffle.py"
    elif scheduling_level == 2:
        shuffle_file += "application_scheduling_off_ver2/push_based_shuffle.py"
    else:
        return

    shutil.copy(shuffle_file, BOA_DIR)
    shutil.copy(shuffle_file, PRODUCTION_DIR)

def read_migration_count():
    migration_count = '0'
    if os.path.isfile("/tmp/ray/migration_count"):
        with open("/tmp/ray/migration_count") as file:
            for line in file:
                migration_count = line
    return migration_count

serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv.bind(('0.0.0.0', PORT))
serv.listen(1)
while True:
  conn, addr = serv.accept()
  while True:
    
    buf = conn.recv(4)
    if not buf: break
    data_size = struct.unpack('>I', buf)[0]
    received_payload = b""
    reamining_payload_size = data_size
    while reamining_payload_size != 0:
        received_payload += conn.recv(reamining_payload_size)
        reamining_payload_size = data_size - len(received_payload)
    data_dict = pickle.loads(received_payload)
    print (data_dict)

    return_str = socket.gethostname()
    if data_dict['shutdown']:
        os.system('ray stop')
        conn.close()
        print ('shutdown')
        quit()
    if data_dict['stop']:
        os.system('ray stop')
        os.system('rm -rf /ray_spill/*')
        return_str=read_migration_count()
    else:
        #RAY_BACKEND_LOG_LEVEL=debug 
        sanitize_data(data_dict)
        os.system('RAY_enable_BlockTasks='+ str(data_dict['BACKPRESSURE']) + ' RAY_enable_BlockTasksSpill=' +
                  str(data_dict['BLOCKSPILL']) + ' RAY_enable_Deadlock2=' + str(data_dict['BLOCKSPILL']) +
                  ' RAY_enable_EagerSpill=' + str(data_dict['EAGERSPILL']) +
                  ' ./up.sh -n ' + data_dict['num_cpus'] + ' -o ' + data_dict['obj_store_size'])
        push_based_shuffle_setup(data_dict['push_based_shuffle_app_scheduling_level'])
        return_str += " Ray Up"
    conn.send(return_str.encode())
  conn.close()
print ('client disconnected and shutdown')
