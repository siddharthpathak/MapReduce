import multiprocessing
import os
import time
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import subprocess
from threading import Thread

def mapper(port):
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
            t = Thread(target=worker,args=(master_url,master_port))
            t.start()
            return 1

        server.serve_forever()

def worker(master_url,master_port):
    print("Mapper worker started working with PID: ", os.getpid())
    # Here the worker will calculate the output
    time.sleep(15)
    # Then write to the output file and send the keys to master
    s = xmlrpc.client.ServerProxy('http://'+master_url+":"+str(master_port))
    s.send_mapper_keys(["a","dog"],os.getpid())
