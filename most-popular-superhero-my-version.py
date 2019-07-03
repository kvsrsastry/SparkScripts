from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Most-Popular-Superhero-My-Version")
sc = SparkContext(conf = conf)

def get_connections(line):
    arr = line.split()
    hero = int(arr[0])
    conns = []
    for conn in arr[1:]:
        conns.append(conn)
    return (hero, conns)

lines = sc.textFile("file:///SparkCourse/Marvel-Graph.txt")
hero_conns = lines.map(get_connections)
hero_conns = hero_conns.reduceByKey(lambda x, y: list(set(x + y)))
hero_num_conns = hero_conns.mapValues(lambda x: len(x))
superhero_tup = hero_num_conns.map(lambda x: (x[1], x[0])).max()

superhero = superhero_tup[1]
superhero_conns = superhero_tup[0]

import re
def get_id_names(line):
    arr = re.split(r'\s+"', line)
    hero_id = int(arr[0])
    hero_name = arr[1]
    hero_name = hero_name.replace('"', '').encode('utf8')
    return (hero_id, hero_name)

lines1 = sc.textFile("file:///SparkCourse/Marvel-Names.txt")
id_names = lines1.map(get_id_names)
superhero_name = id_names.lookup(superhero)[0].decode('utf8')

print('Superhero with ID {} and name {} has max connections i.e., {}'.format(superhero, superhero_name, superhero_conns))
