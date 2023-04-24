import node_info
import socket
import pickle

PORT = node_info.PORT
Worker_Addresses = node_info.Worker_Addresses

data = pickle.dumps({"num_cpus":1, "stop":True, "shutdown":False, 
                     "obj_store_size": 1000_000_000, "BACKPRESSURE":False,
                     "BLOCKSPILL":False, "EAGERSPILL":False,
                     "push_based_shuffle_app_scheduling_level":-1})

def get_migration_count_from_remote():
    clients = []
    counts = 0
    for addr in Worker_Addresses:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((addr, PORT))
        buf = struct.pack('>I', len(data))
        client.sendall(struct.pack('>I', len(data)))
        client.sendall(data)
        clients.append(client)

    for client in clients:
        from_server= client.recv(4096)
        counts += int(from_server.decode())
        client.close()

    print("Remote Migration Count", counts)
    return len(Worker_Addresses), counts    

