import socket
import pickle
import struct
import argparse
import os
import time
import node_info

PORT = node_info.PORT
Worker_Addresses = node_info.Worker_Addresses

################ Get Ray Env Variables ################ 
def boolean_string(s):
    if s not in {'False', 'True', 'false', 'true'}:
        raise ValueError('Not a valid boolean string')
    return (s == 'True' or  s == 'true')

parser = argparse.ArgumentParser()
parser.add_argument('--NUM_CPUS', '-nw', type=int, default=16)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=2_000_000_000)
parser.add_argument('--STOP', '-s', type=boolean_string, default=False)
parser.add_argument('--SHUTDOWN', '-d', type=boolean_string, default=False)
parser.add_argument('--BACKPRESSURE', '-b', type=boolean_string, default=False)
parser.add_argument('--BLOCKSPILL', '-bs', type=boolean_string, default=False)
parser.add_argument('--EAGERSPILL', '-e', type=boolean_string, default=False)
parser.add_argument('--PUSH_BASED_SHUFFLE_APP_SCHEDULING_LEVEL', '-a', type=int, default=-1)
args = parser.parse_args()
params = vars(args)

num_nodes = len(Worker_Addresses) + 1
num_cpus = str(params['NUM_CPUS'] // num_nodes)
object_store_size = str(params['OBJECT_STORE_SIZE'] // num_nodes)
stop = params['STOP']
shutdown = params['SHUTDOWN']
backpressure = params['BACKPRESSURE']
blockspill = params['BLOCKSPILL']
eagerspill = params['EAGERSPILL']
push_based_shuffle_app_scheduling_level = params['PUSH_BASED_SHUFFLE_APP_SCHEDULING_LEVEL']

################ Connect to Worker Nodes ################ 
if not stop and not shutdown:
    os.system('~/OSDI23/script/multinode/up.sh -n ' + num_cpus + ' -o ' + object_store_size)


data = pickle.dumps({"num_cpus":num_cpus, "stop":stop, "shutdown":shutdown, 
                     "obj_store_size": object_store_size, "BACKPRESSURE":backpressure,
                     "BLOCKSPILL":blockspill, "EAGERSPILL":eagerspill,
                     "push_based_shuffle_app_scheduling_level":push_based_shuffle_app_scheduling_level})

clients = []
for addr in Worker_Addresses:
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    connection_made = False
    while not connection_made:
        try:
            client.connect((addr, PORT))
            connection_made = True
        except socket.error as msg:
            print(addr,PORT)
            print(msg)
            time.sleep(3)
    buf = struct.pack('>I', len(data))
    client.sendall(struct.pack('>I', len(data)))
    client.sendall(data)

    clients.append(client)

for client in clients:
    from_server = client.recv(4096)
    client.close()
    print (from_server.decode())

