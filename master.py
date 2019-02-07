import multiprocessing
import os
import time
import socketserver
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import subprocess
import json
from mapper import mapper
from reducer import reducer


def split_file(input_file_name, number_of_mappers):
    with open(input_file_name) as input_file:
        for line_count, line in enumerate(input_file):
            pass
        print("Got line count of ", line_count+1)
        section_size = (line_count+1)//number_of_mappers
        sections = [[section_size*i, section_size*(i+1)-1] for i in range(0, number_of_mappers)]
        sections[-1][1] += (line_count+1) % number_of_mappers

        return sections


def start_master_server(config_file):
    class ThreadedXMLRPCServer(socketserver.ThreadingMixIn, SimpleXMLRPCServer):
        pass

    class RequestHandler(SimpleXMLRPCRequestHandler):
        rpc_paths = ('/RPC2',)

    mappers = []
    mappers_completed = []
    reducers = []
    reducers_completed = []
    keys = []
    final_result = []
    master_ip = json.load(open(config_file))["master_ip"]
    master_port = json.load(open(config_file))["master_port"]

    with ThreadedXMLRPCServer((master_ip, master_port), requestHandler=RequestHandler, logRequests=False) as server:
        server.register_introspection_functions()

        @server.register_function
        def spawn_mappers(config_file):
            """
                Starts mappers as per the input config file
            """
            input_mappers = json.load(open(config_file))["mappers"]
            for m in input_mappers:
                p1 = multiprocessing.Process(target=mapper, args=(m["port"],))
                p1.start()
                mappers.append(p1.pid)
                time.sleep(2)
            return 1

        @server.register_function
        def spawn_reducers(config_file):
            """
                Starts reducers as per the input config file
            """

            input_reducers = json.load(open(config_file))["reducers"]
            for r in input_reducers:
                p1 = multiprocessing.Process(target=reducer, args=(r["port"],))
                p1.start()
                reducers.append(p1.pid)
                time.sleep(2)
            return 1

        @server.register_function
        def start_job(config_file):
            """
                Starts map reduce job as per the input file specified in the config
                Tells each mapper the section of the file it has to work on
                Reads mapper IP and port from the config file
            """

            input_config = json.load(open(config_file))
            input_mappers = input_config["mappers"]

            print("Splitting the input file", input_config["input_file"])
            # Split the file depending on the number of lines
            sections = split_file(input_config["input_file"], len(input_mappers))

            # Now tell each mapper to start working on their part
            # We contact each mapper using RPC using IP and port from the config file
            for i, m in enumerate(input_mappers):
                subprocess.check_call(["mkdir", "-p", "./tmp/"+str(mappers[i])])
                subprocess.check_call(["cp", input_config["input_file"], "./tmp/"+str(mappers[i])+"/"+input_config["input_file"]])
                s = xmlrpc.client.ServerProxy('http://'+m["ip"]+":"+str(m["port"]))
                # Here we also need to pass the mapper function 
                s.start_working(master_ip, master_port, input_config["input_file"], sections[i])

            # Keep checking if all the reducers are completed
            # Need to run this in while 1, then we can reply to client that we are done
            while 1:
                if len(reducers) == len(reducers_completed):
                    print("All reducers completed..")
                    return final_result

        @server.register_function
        def send_mapper_keys(received_keys, pid):
            """
                It is called by mappers to send their keys
                Once the master receives all the keys it tells the reducers to start the job
            """
            keys.extend(received_keys)
            mappers_completed.append(pid)

            if len(mappers) == len(mappers_completed):
                print("All mappers completed..forming keys")
                keys_set = list(set(keys))
                input_reducers = json.load(open(config_file))["reducers"]
                input_mappers = [(d["ip"], d["port"]) for d in (json.load(open(config_file))["mappers"])]
                reducer_keys = [[] for _ in range(0, len(input_reducers))]

                print("Assigning keys to reducers")
                kpr = len(keys_set)//len(input_reducers)
                for i in range(0, len(input_reducers)):
                    for j in range(i*kpr, i*kpr + kpr):
                        reducer_keys[i].append(keys_set[j])

                rem_keys = len(keys_set) % len(input_reducers)
                if rem_keys > 0:
                    for k in keys_set[-rem_keys:]:
                        reducer_keys[-1].append(k)

                print("Starting reducers")
                for i, r in enumerate(input_reducers):
                    s = xmlrpc.client.ServerProxy('http://'+r["ip"]+":"+str(r["port"]))
                    # Here we also need to pass the reducer function
                    # And url and port of master
                    s.start_working(input_mappers, reducer_keys[i], master_ip, master_port)

            return 1

        @server.register_function
        def send_reducer_keys(result, pid):
            """
                Called by the reducers to send their [key,value] pair to the master
            """
            final_result.extend(result)
            reducers_completed.append(pid)
            print("One reducer completed..")
            return 1

        print("Starting master server with PID", os.getpid())
        server.serve_forever()


if __name__ == '__main__':
    # Start the master server before starting all the mappers
    # Read the port from config file
    start_master_server("config.json")
