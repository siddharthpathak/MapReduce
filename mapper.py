import os
import time
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
from threading import Thread
import json
import pickle
import socketserver
import subprocess


files_list = []


def wc_input_map_func(d,line):
    output = []
    words = line.split()
    for w in words:
        w = w.lower()
        output.append((w, 1))

    return output


def wc_combiner(output):

    output_count = {}
    for k, v in output:
        output_count[k] = output_count.get(k, 0) + 1

    return [(k, v) for k, v in output_count.items()]


def inverted_input_map_func(d,line):
    output = []
    words = line.split()
    for w in words:
        w = w.lower()
        output.append((w, d))

    return output


def inverted_combiner(output):

    output_count = {}
    for k, v in output:
        if k in output_count:
            if v in output_count[k]:
                output_count[k][v] += 1
            else:
                output_count[k][v] = 1
        else:
            output_count[k] = {v: 1}

    result = []
    for k, v in output_count.items():
        temp = []
        for k2, v2 in v.items():
            temp.append((k2, v2))
        result.append((k, temp))

    return result


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
        def start_working(master_url, master_port, section, r, func):
            t = Thread(target=worker, args=(master_url, master_port, section, r, func))
            t.start()
            return 1

        @server.register_function
        def get_keys(keys):
            for f in files_list:
                if f == keys:
                    with open("./tmp/"+str(os.getpid())+"/"+f+".txt", "rb") as output_file:
                        return pickle.load(output_file)
            return 0

        @server.register_function
        def destroy_mapper():
            print("Killing the mapper..deleting local files")
            subprocess.check_call(["rm", "-rf", "./tmp/" + str(os.getpid())])
            t = Thread(target=shutdown, args=(server,))
            t.start()
            return 1

        server.serve_forever()


def worker(master_url, master_port, section, r, func):
    print("Mapper worker started working with PID: ", os.getpid())
    in_out = []
    if func == "word_count":
        input_map_func = wc_input_map_func
        combiner = wc_combiner
    else:
        input_map_func = inverted_input_map_func
        combiner = inverted_combiner

    for f, s in section:
        with open("./tmp/"+str(os.getpid())+"/"+f) as input_file:
            ip_string = []
            for line_number, line in enumerate(input_file):
                if s[0] <= line_number <= s[1]:
                    ip_string.append(line)
            in_out.extend(input_map_func(f, " ".join(ip_string)))

    in_out = combiner(in_out)
    file_keys = {}
    for k, v in in_out:
        temp_hash = str(hash(k) % r)
        if temp_hash in file_keys:
            file_keys[temp_hash].append((k, v))
        else:
            file_keys[temp_hash] = [(k, v)]

    # Write to the output file and send the keys to master
    for f in file_keys.keys():
        with open("./tmp/"+str(os.getpid())+"/"+f+".txt", "wb+") as output_file:
            pickle.dump(file_keys[f], output_file)
            files_list.append(f)
    s = xmlrpc.client.ServerProxy('http://'+master_url+":"+str(master_port))
    s.send_mapper_keys(files_list, os.getpid())
