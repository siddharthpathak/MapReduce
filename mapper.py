import os
import time
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
from threading import Thread
import json
import socketserver
import subprocess


def input_map_func(line):
    output_count = {}
    words = line.split()
    for w in words:
        w = w.lower()
        output_count[w] = output_count.get(w, 0) + 1

    return output_count


def shutdown(server):
    server.shutdown()


def mapper(ip, port):
    print("Mapper listening on port ", port)

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
        def start_working(master_url, master_port, section):
            t = Thread(target=worker, args=(master_url, master_port, section))
            t.start()
            return 1

        @server.register_function
        def get_keys(key):
            with open("./tmp/"+str(os.getpid())+"/in_output.txt") as output_file:
                return json.load(output_file).get(key, 0)

        @server.register_function
        def destroy_mapper():
            print("Killing the mapper..deleting local files")
            subprocess.check_call(["rm", "-rf", "./tmp/" + str(os.getpid())])
            t = Thread(target=shutdown, args=(server,))
            t.start()
            return 1

        server.serve_forever()


def worker(master_url, master_port, section):
    print("Mapper worker started working with PID: ", os.getpid())
    print("I will work on", section)
    ip_string = []

    for f, s in section:
        with open("./tmp/"+str(os.getpid())+"/"+f) as input_file:
            for line_number, line in enumerate(input_file):
                if s[0] <= line_number <= s[1]:
                    ip_string.append(line)

    output_count = input_map_func(" ".join(ip_string))
    # Write to the output file and send the keys to master
    with open("./tmp/"+str(os.getpid())+"/in_output.txt", "w+") as output_file:
        json.dump(output_count, output_file, indent=4)
    s = xmlrpc.client.ServerProxy('http://'+master_url+":"+str(master_port))
    s.send_mapper_keys(list(output_count.keys()), os.getpid())
