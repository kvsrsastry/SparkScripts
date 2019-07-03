from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Degrees-of-Separation-my-version")
sc = SparkContext(conf = conf)

src = 5306 #SpiderMan
dest = 14  #ADAM 3,031 (who?)

hit = sc.accumulator(0)

def get_nodes(line):
    arr = line.split()
    hero = int(arr[0])
    conns = []
    for conn in arr[1:]:
        conns.append(int(conn))
    visited = 0
    dist = float("inf")
    if hero == src:
        visited = 1
        dist = 0

    return (hero, (conns, visited, dist))

def merge_conns(x, y):
    conns = list(set(x[0] + y[0]))
    visited = x[1]
    dist = x[2]
    return (conns, visited, dist)

def process_ready_to_visit_nodes(tup):
    hero = tup[0]
    conns = tup[1][0]
    visited = tup[1][1]
    dist = tup[1][2]
    result = []
    if visited == 1:
        for conn in conns:
            new_visited = 1
            new_dist = dist + 1
            result.append((conn, ([], new_visited, new_dist)))
            if conn == dest:
                hit.add(1)
        visited = 2
    result.append((hero, (conns, visited, dist)))
    return result

lines = sc.textFile("file:///SparkCourse/Marvel-Graph.txt")
nodes = lines.map(get_nodes)
print('Number of lines in the Marvel-Graph to start with : {}'.format(nodes.count()))
nodes = nodes.reduceByKey(merge_conns)
print('Number of unique nodes to start with : {}'.format(nodes.count()))

def merge_nodes(x, y):
    conns1 = x[0]
    conns2 = y[0]
    visited1 = x[1]
    visited2 = y[1]
    dist1 = x[2]
    dist2 = y[2]
    
    conns = list(set(conns1 + conns2))
    visited = max(visited1, visited2)
    dist = min(dist1, dist2)
    
    return (conns, visited, dist)

for cnt in range(10):
    print('Iteration #{}'.format(cnt+1))
    nodes = nodes.flatMap(process_ready_to_visit_nodes)
    print('\tFlatMap contains {} nodes before merging'.format(nodes.count()))
    nodes = nodes.reduceByKey(merge_nodes)
    print('\tFlatMap contains {} nodes after merging'.format(nodes.count()))
    if hit.value > 0:
        print(nodes.lookup(dest)[0])
        break
