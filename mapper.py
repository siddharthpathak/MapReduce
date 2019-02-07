import os
import time
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
from threading import Thread
import json
import socketserver


def input_map_func(line, output_count):
    words = line.split()
    for w in words:
        w = w.lower()
        output_count[w] = output_count.get(w, 0) + 1


def mapper(port):
    print("Mapper listening on port ", port)

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
        def start_working(master_url, master_port, input_file_name, section):
            t = Thread(target=worker, args=(master_url, master_port, input_file_name, section))
            t.start()
            return 1

        @server.register_function
        def get_keys(key):
            with open("./tmp/"+str(os.getpid())+"/in_output.txt") as output_file:
                return json.load(output_file).get(key, 0)

        server.serve_forever()


def worker(master_url, master_port, input_file_name, section):
    print("Mapper worker started working with PID: ", os.getpid())
    print("I will work on", section)
    output_count = {}

    with open("./tmp/"+str(os.getpid())+"/"+input_file_name) as input_file:
        for line_number, line in enumerate(input_file):
            if section[0] <= line_number <= section[1]:
                input_map_func(line, output_count)

    # Write to the output file and send the keys to master
    with open("./tmp/"+str(os.getpid())+"/in_output.txt", "w+") as output_file:
        json.dump(output_count, output_file, indent=4)
    s = xmlrpc.client.ServerProxy('http://'+master_url+":"+str(master_port))
    s.send_mapper_keys(list(output_count.keys()), os.getpid())
