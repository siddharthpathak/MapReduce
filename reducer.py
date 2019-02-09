import os
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
from threading import Thread
import socketserver
import subprocess
import time


def reducer_func(keys):
    output = {}
    for t in keys:
        output[t[0]] = output.get(t[0], 0) + t[1]

    return output


def shutdown(server):
    server.shutdown()


def reducer(ip, port):
    """
        Starts reducer RPC server on the specified port
    """

    print("Reducer listening on port ", port)

    class ThreadedXMLRPCServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer):
        pass

    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)

    with ThreadedXMLRPCServer((ip, port), requestHandler=RequestHandler, logRequests=False) as server:
        server.register_introspection_functions()

        @server.register_function
        def check_if_alive():
            return 1

        @server.register_function
        def start_working(input_mappers, keys, master_url, master_port):
            """
                Starts a new thread for the reducer worker.

            """
            t = Thread(target=worker, args=(input_mappers, keys, master_url, master_port))
            t.start()
            return 1

        @server.register_function
        def destroy_reducer():
            print("Killing the reducer..")
            t = Thread(target=shutdown, args=(server,))
            t.start()
            return 1

        server.serve_forever()


def worker(mappers, allotted_keys, master_url, master_port):
    """
        Reducer worker function. Calls the reduce function after fetching keys from all the mappers
        Master sends the list of mappers to the reducer
    """
    # Here the worker will calculate the output
    # Ask the mappers for the key
    print("Reducer started working with PID", os.getpid())
    in_output = []
    for m in mappers:
        s = xmlrpc.client.ServerProxy('http://' + m[0] + ":" + str(m[1]))
        for k in allotted_keys:
            temp_key = s.get_keys(k)
            if temp_key:
                in_output.append([k, temp_key])

    final_output = reducer_func(in_output)
    result = [(k, v) for k, v in final_output.items()]
    s = xmlrpc.client.ServerProxy('http://'+master_url+":"+str(master_port))
    s.send_reducer_keys(result, os.getpid())
