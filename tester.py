import xmlrpc.client
import sys
import json

if __name__ == '__main__':

    input_config = json.load(open(sys.argv[1]))
    master_ip = input_config["master_ip"]
    master_port = input_config["master_port"]
    func = input_config["map_func"]
    s = xmlrpc.client.ServerProxy('http://' + master_ip + ":" + str(master_port))

    print("Initializing cluster..")
    s.init_cluster(sys.argv[1])

    print("Starting job...running", input_config["map_func"])
    map_reduce_output = (s.start_job("config.json"))

    if func == "word_count":
        seq_output_count = {}
        for f in (input_config["input_files"]):
            with open(f,encoding='utf-8', errors='replace') as input_file:
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
            with open(f,encoding='utf-8', errors='replace') as input_file:
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
        map_reduce_count = {}

        for t in map_reduce_output:
            map_reduce_count[t[0]] = {}
            for x in t[1]:
                map_reduce_count[t[0]][x[0]] = x[1]

        if seq_output_count == map_reduce_count:
            print("Output Matched!!!")
        else:
            print("Not matched")

    print("Killing the cluster")
    s.destroy_cluster()


