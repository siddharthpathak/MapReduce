import os
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
from threading import Thread
import socketserver
import subprocess
import time


def wc_reducer_func(keys):
    output = {}
    for t in keys:
        output[t[0]] = output.get(t[0], 0) + t[1]

    return [(k, v) for k, v in output.items()]


def inverted_reducer_func(keys):

    output = {}
    for t in keys:
        if t[0] not in output:
            output[t[0]] = {}
        for x in t[1]:
            if x[0] in output[t[0]]:
                output[t[0]][x[0]] += x[1]
            else:
                output[t[0]][x[0]] = x[1]
    final = []
    for k, v in output.items():
        temp = []
        for k2, v2 in v.items():
            temp.append((k2, v2))
        final.append((k, temp))

    return final


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
        def start_working(input_mappers, keys, master_url, master_port, func):
            """
                Starts a new thread for the reducer worker.

            """
            t = Thread(target=worker, args=(input_mappers, keys, master_url, master_port, func))
            t.start()
            return 1

        @server.register_function
        def destroy_reducer():
            print("Killing the reducer..")
            t = Thread(target=shutdown, args=(server,))
            t.start()
            return 1

        server.serve_forever()


def worker(mappers, allotted_keys, master_url, master_port, func):
    """
        Reducer worker function. Calls the reduce function after fetching keys from all the mappers
        Master sends the list of mappers to the reducer
    """
    # Here the worker will calculate the output
    # Ask the mappers for the key
    print("Reducer started working with PID", os.getpid())
    if func == "word_count":
        reducer_func = wc_reducer_func
    else:
        reducer_func = inverted_reducer_func
    in_output = []

    for m in mappers:
        s = xmlrpc.client.ServerProxy('http://' + m[0] + ":" + str(m[1]))
        temp = s.get_keys(allotted_keys)
        if temp != 0:
            in_output.extend(temp)
    result = reducer_func(in_output)
    s = xmlrpc.client.ServerProxy('http://'+master_url+":"+str(master_port))
    s.send_reducer_keys(result, os.getpid())
