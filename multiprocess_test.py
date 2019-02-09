import xmlrpc.client

if __name__ == '__main__':
    mappers = []
    mappers_completed = []
    s = xmlrpc.client.ServerProxy('http://localhost:6000')
    s.spawn_mappers("config.json")
    print("Mappers Initialzed..")

    s.spawn_reducers("config.json")
    print("Reducers Intialized")

    print("Starting job...")
    map_reduce_output= (s.start_job("config.json"))

    seq_output_count = {}
    with open("sherlock.txt") as input_file:
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
            print("Output not matched for key", w[0])
        map_reduce_count[w[0]]= w[1]

    if seq_output_count == map_reduce_count:
        print("Output Matched!!!")

    print("Killing the cluster")
    s.destroy_cluster()


