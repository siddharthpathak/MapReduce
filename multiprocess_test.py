import xmlrpc.client

if __name__ == '__main__':
    mappers = []
    mappers_completed = []
    s = xmlrpc.client.ServerProxy('http://localhost:6000')
    s.spawn_mappers("config.json")

    s.spawn_reducers("config.json")

    s.start_job("config.json")

    output_count = {}
    with open("sherlock.txt") as input_file:
        for line_count, line in enumerate(input_file):
            words = line.split()
            for w in words:
                w = w.lower()
                output_count[w] = output_count.get(w, 0) + 1

    print("Sequential: ", output_count)



