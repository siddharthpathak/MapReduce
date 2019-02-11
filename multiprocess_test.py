import xmlrpc.client
import sys
import json

if __name__ == '__main__':

    input_config = json.load(open(sys.argv[1]))
    master_ip = input_config["master_ip"]
    master_port = input_config["master_port"]
    func = input_config["map_func"]
    s = xmlrpc.client.ServerProxy('http://' + master_ip + ":" + str(master_port))

    print("Starting mappers")
    s.spawn_mappers("config.json")
    print("Mappers Initialzed..")

    print("Starting reducers")
    s.spawn_reducers("config.json")
    print("Reducers Intialized")

    print("Starting job...")
    map_reduce_output = (s.start_job("config.json"))

    if func == "word_count":
        seq_output_count = {}
        for f in (input_config["input_files"]):
            with open(f) as input_file:
                for line_count, line in enumerate(input_file):
                    words = line.split()
                    for w in words:
                        w = w.lower()
                        seq_output_count[w] = seq_output_count.get(w, 0) + 1

        map_reduce_count = {}
        for w in map_reduce_output:
            if seq_output_count[w[0]] == w[1]:
                pass
            else:
                print("Output not matched for key", w[0],w[1],seq_output_count[w[0]])
            map_reduce_count[w[0]] = w[1]

        if seq_output_count == map_reduce_count:
            print("Output Matched!!!")
        else:
            print("Not matched")

    else:
        seq_output_count = {}
        for f in (input_config["input_files"]):
            with open(f) as input_file:
                for line_count, line in enumerate(input_file):
                    words = line.split()
                    for w in words:
                        w = w.lower()
                        if w in seq_output_count:
                            if f in seq_output_count[w]:
                                seq_output_count[w][f] = seq_output_count[w][f] + 1
                            else:
                                seq_output_count[w][f] = 1
                        else:
                            seq_output_count[w] = {f: 1}

        print(seq_output_count)
        print(map_reduce_output)

    print("Killing the cluster")
    s.destroy_cluster()


