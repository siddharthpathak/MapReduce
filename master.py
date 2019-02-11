import multiprocessing
import os
import time
import socketserver
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
import xmlrpc.client
import subprocess
import json
import sys
from mapper import mapper
from reducer import reducer


def split_file(input_file_name, number_of_mappers):
    with open(input_file_name, encoding='utf-8', errors='replace') as input_file:
        for line_count, line in enumerate(input_file):
            pass
        section_size = (line_count+1)//number_of_mappers
        sections = [[section_size*i, section_size*(i+1)-1] for i in range(0, number_of_mappers)]
        sections[-1][1] += (line_count+1) % number_of_mappers

        return sections


def start_reducer_job(master_ip, master_port, config_file, keys, func):

    print("All mappers completed..forming keys")
    keys_set = list(set(keys))
    input_reducers = json.load(open(config_file))["reducers"]
    input_mappers = [(d["ip"], d["port"]) for d in (json.load(open(config_file))["mappers"])]
    reducer_keys = []

    print("Assigning keys to reducers")
    for i in range(0, len(keys_set)):
        reducer_keys.append((input_reducers[i]["ip"], input_reducers[i]["port"], keys_set[i]))

    print("Starting reducers")
    for i in reducer_keys:
        s = xmlrpc.client.ServerProxy('http://'+i[0]+":"+str(i[1]))
        # Here we also need to pass the reducer function
        s.start_working(input_mappers, i[2], master_ip, master_port, func)

    return reducer_keys


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
    input_config = json.load(open(config_file))
    master_ip = input_config["master_ip"]
    master_port = input_config["master_port"]

    with ThreadedXMLRPCServer((master_ip, master_port), requestHandler=RequestHandler, logRequests=False) as server:

        def init_cluster(config_file):
            spawn_mappers(config_file)
            spawn_reducers(config_file)
            return 1
        server.register_function(init_cluster)

        def spawn_mappers(config_file):
            """
                Starts mappers as per the input config file
            """
            input_mappers = json.load(open(config_file))["mappers"]
            for m in input_mappers:
                p1 = multiprocessing.Process(target=mapper, args=(m["ip"], m["port"]))
                p1.start()
                mappers.append(p1.pid)
                time.sleep(2)
            return 1
        server.register_function(spawn_mappers)

        def spawn_reducers(config_file):
            """
                Starts reducers as per the input config file
            """

            input_reducers = json.load(open(config_file))["reducers"]
            for r in input_reducers:
                p1 = multiprocessing.Process(target=reducer, args=(r["ip"], r["port"]))
                p1.start()
                reducers.append(p1.pid)
                time.sleep(2)
            return 1
        server.register_function(spawn_reducers)

        def start_job(config_file):
            """
                Starts map reduce job as per the input file specified in the config
                Tells each mapper the section of the file it has to work on
                Reads mapper IP and port from the config file
            """

            input_config = json.load(open(config_file))
            input_mappers = input_config["mappers"]
            input_reducers = input_config["reducers"]
            input_files = input_config["input_files"]
            func = input_config["map_func"]
            output_location = input_config["output_location"]
            sections = []

            for f, ipf in enumerate(input_files):
                print("Splitting the input file", ipf)
                # Split the file depending on the number of lines
                sections.append((ipf, split_file(ipf, len(input_mappers))))

            # Now tell each mapper to start working on their part
            # We contact each mapper using RPC using IP and port from the config file
            for i, m in enumerate(input_mappers):
                subprocess.check_call(["mkdir", "-p", "./tmp/"+str(mappers[i])])
                for f, ipf in enumerate(input_files):
                    subprocess.check_call(["cp", ipf, "./tmp/"+str(mappers[i])+"/"+ipf])
                s = xmlrpc.client.ServerProxy('http://'+m["ip"]+":"+str(m["port"]))
                temp_section = [(f, s[i]) for f, s in sections]
                # Here we also need to pass the mapper function
                s.start_working(master_ip, master_port, temp_section, len(reducers), func)

            # Keep checking if all the mappers have completed
            # Else we will ask for heartbeat every 4 seconds
            while 1:
                time.sleep(4)
                if len(mappers) == len(mappers_completed):
                    print("All mappers completed..")
                    actual_reducers = start_reducer_job(master_ip, master_port, config_file, keys, func)
                    break

                for i, m in enumerate(input_mappers):
                    s = xmlrpc.client.ServerProxy('http://'+m["ip"]+":"+str(m["port"]))
                    try:
                        s.check_if_alive()
                    except:
                        print("Mapper "+str(i)+" crashed..starting the job again")
                        destroy_cluster()
                        time.sleep(5)
                        spawn_mappers(config_file)
                        spawn_reducers(config_file)
                        return start_job(config_file)

            # Keep checking if all the reducers are completed
            # Need to run this in while 1, then we can reply to client that we are done
            while 1:
                time.sleep(4)
                if len(actual_reducers) == len(reducers_completed):
                    print("All reducers completed..writing to file")
                    #with open(output_location, "w+") as output_file:
                    #    output_file.write(final_result)
                    return final_result

                # Keep checking if all the reducers have completed the job
                for i in actual_reducers:
                    s = xmlrpc.client.ServerProxy('http://'+i[0]+":"+str(i[1]))
                    try:
                        s.check_if_alive()
                    except:
                        print("Reducer "+str(i)+" crashed..starting the job again")
                        destroy_cluster()
                        time.sleep(5)
                        spawn_mappers(config_file)
                        spawn_reducers(config_file)
                        return start_job(config_file)
        server.register_function(start_job)

        def send_mapper_keys(received_keys, pid):
            """
                It is called by mappers to send their keys
                Once the master receives all the keys it tells the reducers to start the job
            """
            keys.extend(received_keys)
            mappers_completed.append(pid)
            return 1
        server.register_function(send_mapper_keys)

        def send_reducer_keys(result, pid):
            """
                Called by the reducers to send their [key,value] pair to the master
            """
            final_result.extend(result)
            reducers_completed.append(pid)
            print("One reducer completed..")
            return 1
        server.register_function(send_reducer_keys)

        def destroy_cluster():
            print("Killing the cluster")
            input_config = json.load(open(config_file))
            input_reducers = input_config["reducers"]
            del mappers[:]
            del mappers_completed[:]
            del reducers[:]
            del reducers_completed[:]
            for i, r in enumerate(input_reducers):
                s = xmlrpc.client.ServerProxy('http://'+r["ip"]+":"+str(r["port"]))
                try:
                    s.destroy_reducer()
                except:
                    print("Reducer already dead!")

            input_mappers = input_config["mappers"]
            for i, m in enumerate(input_mappers):
                s = xmlrpc.client.ServerProxy('http://'+m["ip"]+":"+str(m["port"]))
                try:
                    s.destroy_mapper()
                except:
                    print("Mapper already dead!")

            return 1
        server.register_function(destroy_cluster)

        print("Starting master server with PID", os.getpid())
        server.serve_forever()


if __name__ == '__main__':
    start_master_server(sys.argv[1])
