import multiprocessing
import os
import time
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
from threading import Thread

def start_master_server(port):
    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)

    with SimpleXMLRPCServer(('localhost', 8000),
                            requestHandler=RequestHandler,logRequests=False) as server:
        server.register_introspection_functions()

        @server.register_function
        def send_keys(keys,pid):
            mappers_completed.append(pid)
            if len(mappers) == len(mappers_completed):
                print ("All mappers completed..now starting the reducers")
            return 1

        print("Starting master server with PID", os.getpid())
        server.serve_forever()
    
def mapper_worker(master_url,master_port):
    print("Mapper worker started working with PID: ", os.getpid())
    # Here the worker will calculate the output
    time.sleep(15)
    # Then write to the output file and send the keys to master
    s = xmlrpc.client.ServerProxy('http://'+master_url+":"+str(master_port))
    s.send_keys(["a","dog"],os.getpid())

def reducer():
    # Will get the keys from each mapper, reduce them
    pass

def mapper(port):
    print("Hello from mapper ", os.getpid())
    print("Mapper listening on port ", port)
    # Restrict to a particular path.
    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)

    with SimpleXMLRPCServer(('localhost', port),
                            requestHandler=RequestHandler,logRequests=False) as server:
        server.register_introspection_functions()

        @server.register_function
        def check_if_alive():
            return 1
 
        @server.register_function
        def start_working(master_url,master_port):
            t = Thread(target=mapper_worker,args=(master_url,master_port))
            t.start()
            return 1
    
        server.serve_forever()

def spawn_mappers(mappers):
    # Start a new process with target as mapper function
    # Number and port should be read from config file
    for m in range(0,1):
        p1 = multiprocessing.Process(target=mapper,args=(9000+m,))
        p1.start()
        mappers.append(p1.pid)
        # Here we need to copy the input file to directory /pid/input.txt
    
    # Now tell each mapper to start working on their part
    for m in range(0,2):
        s = xmlrpc.client.ServerProxy('http://localhost:'+str(9000+m))
        # Here we also need to pass the mapper function 
        # And url and port of master
        s.start_working("localhost",8000)
 
if __name__=='__main__':
    mappers = []
    mappers_completed = []
    s= xmlrpc.client.ServerProxy('http://localhost:8000')
    s.spawn_mappers()

    s.spawn_reducers()

    s.start_job()

