import socket
import pickle
import os

PORT=6380

serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
serv.bind(('0.0.0.0', PORT))
serv.listen(1)
while True:
  conn, addr = serv.accept()
  while True:
    data = conn.recv(4096)
    if not data: break
    data_dict = pickle.loads(data)
    print (data_dict)
    os.system('./up.sh -n ' + data_dict['num_cpus'] + ' -o ' + data_dict['obj_store_size'])
    return_str = socket.gethostname() + " Ray up"
    conn.send(return_str.encode())
  conn.close()
print ('client disconnected and shutdown')
