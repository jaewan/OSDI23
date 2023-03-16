import socket
import pickle
import os

os.environ['RAY_worker_lease_timeout_milliseconds']=0
os.environ['RAY_object_spilling_threshold']=1.0
os.environ['RAY_block_tasks_threshold']=1.0
os.environ['RAY_worker_cap_enabled']=False

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
    return_str = socket.gethostname()
    if data_dict['shutdown']:
        os.system('ray stop')
        conn.close()
        print ('shutdown')
        quit()
    if data_dict['stop']:
        os.system('ray stop')
        return_str += " Ray Stop"
    else:
        #RAY_BACKEND_LOG_LEVEL=debug 
        os.system('RAY_enable_BlockTasks='+ str(data_dict['BACKPRESSURE']) + ' RAY_enable_BlockTasksSpill=' +
                  str(data_dict['BLOCKSPILL']) + ' RAY_enable_Deadlock2=' + str(data_dict['BLOCKSPILL']) +
                  ' RAY_enable_EagerSpill=' + str(data_dict['EAGERSPILL']) +
                  ' ./up.sh -n ' + data_dict['num_cpus'] + ' -o ' + data_dict['obj_store_size'])
        return_str += " Ray Up"
    conn.send(return_str.encode())
  conn.close()
print ('client disconnected and shutdown')
