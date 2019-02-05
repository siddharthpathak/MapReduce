import multiprocessing
import os
import time
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import subprocess
from threading import Thread
from mapper import mapper
from reducer import reducer

def start_master_server(port):
    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)

    mappers = []
    mappers_completed = []
    reducers = []
    reducers_completed = []
    keys = []

    with SimpleXMLRPCServer(('localhost', 8000),
                            requestHandler=RequestHandler,logRequests=False) as server:
        server.register_introspection_functions()

        @server.register_function
        def spawn_mappers():
            # Start a new process with target as mapper function
            # Number and port should be read from config file
            for m in range(0,2):
                p1 = multiprocessing.Process(target=mapper,args=(9000+m,))
                p1.start()
                mappers.append(p1.pid)
                time.sleep(2)
            return 1

        @server.register_function
        def spawn_reducers():
            # Start a new process with target as mapper function
            # Number and port should be read from config file
            for m in range(0,2):
                p1 = multiprocessing.Process(target=reducer,args=(10000+m,))
                p1.start()
                reducers.append(p1.pid)
                time.sleep(2)
            return 1

        @server.register_function
        def start_job():
            # Now tell each mapper to start working on their part
            for m in range(0,2):
                subprocess.check_call(["mkdir","-p","./tmp/"+str(mappers[m])])
                subprocess.check_call(["cp","input.txt","./tmp/"+str(mappers[m])+"/input.txt"])
                s = xmlrpc.client.ServerProxy('http://localhost:'+str(9000+m))
                # Here we also need to pass the mapper function 
                # And url and port of master
                s.start_working("localhost",8000)

            return 1

        @server.register_function
        def send_mapper_keys(received_keys,pid):
            keys.extend(received_keys)
            print ("Current keys set", keys)
            mappers_completed.append(pid)
            if len(mappers) == len(mappers_completed):
                print ("All mappers completed..forming keys")
                print ("Key set is ", set(keys))
                print("Starting reducers")
                for m in range(0,2):
                    s = xmlrpc.client.ServerProxy('http://localhost:'+str(10000+m))
                    # Here we also need to pass the reducer function
                    # And url and port of master
                    s.start_working("localhost",8000)

            return 1

        @server.register_function
        def send_reducer_keys(result,pid):
            reducers_completed.append(pid)
            if len(reducers) == len(reducers_completed):
                print("All reducers completed..")
                print("Output is:")
            return 1

        print("Starting master server with PID", os.getpid())
        server.serve_forever()


if __name__=='__main__':
    # Start the master server before starting all the mappers
    # Read the port from config file
    start_master_server(8000)
