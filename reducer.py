import os
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
from threading import Thread
import socketserver


def reducer(port):
    """
        Starts reducer RPC server on the specified port
    """

    print("Reducer listening on port ", port)

    class ThreadedXMLRPCServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer):
        pass

    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)

    with ThreadedXMLRPCServer(('localhost', port), requestHandler=RequestHandler, logRequests=False) as server:
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

        server.serve_forever()


def worker(mappers, allotted_keys, master_url, master_port):
    """
        Reducer worker function. Calls the reduce function after fetching keys from all the mappers
        Master sends the list of mappers to the reducer
    """
    # Here the worker will calculate the output
    # Ask the mappers for the key
    print("Reducer started working")
    final_output = {}
    for m in mappers:
        s = xmlrpc.client.ServerProxy('http://' + m[0] + ":" + str(m[1]))
        for k in allotted_keys:
            final_output[k] = final_output.get(k, 0) + s.get_keys(k)

    result = [(k, v) for k, v in final_output.items()]
    # Then write to the output file and send the keys to master
    s = xmlrpc.client.ServerProxy('http://'+master_url+":"+str(master_port))
    s.send_reducer_keys(result, os.getpid())
