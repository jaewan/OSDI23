import socket
import pickle
import argparse
import os

PORT=6380
Worker_Addresses = ['34.82.222.146']

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

if not stop and not shutdown:
    os.system('~/OSDI23/script/multinode/up.sh -n ' + num_cpus + ' -o ' + object_store_size)

data = pickle.dumps({"num_cpus":num_cpus, "stop":stop, "shutdown":shutdown, 
                     "obj_store_size": object_store_size, "BACKPRESSURE":backpressure,
                     "BLOCKSPILL":blockspill, "EAGERSPILL":eagerspill})
for addr in Worker_Addresses:
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((addr, PORT))
    client.send(data)
    from_server = client.recv(4096)
    client.close()
    print (from_server.decode())