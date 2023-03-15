import socket
import pickle
import argparse
import os

PORT=6380
Worker_Addresses = ['34.83.24.103']

parser = argparse.ArgumentParser()
parser.add_argument('--NUM_CPUS', '-nw', type=int, default=16)
parser.add_argument('--OBJECT_STORE_SIZE', '-o', type=int, default=2_000_000_000)
args = parser.parse_args()
params = vars(args)

num_nodes = len(Worker_Addresses) + 1
num_cpus = str(params['NUM_CPUS'] // num_nodes)
object_store_size = str(params['OBJECT_STORE_SIZE'] // num_nodes)

os.system('~/OSDI23/script/multinode/up.sh -n ' + num_cpus + ' -o ' + object_store_size)

data = pickle.dumps({"obj_store_size": object_store_size, "num_cpus":num_cpus})
for addr in Worker_Addresses:
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((addr, PORT))
    client.send(data)
    from_server = client.recv(4096)
    client.close()
    print (from_server.decode())
