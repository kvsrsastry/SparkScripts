from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("min_temp_my_version")
sc = SparkContext(conf = conf)

def get_req_columns(line):
    arr = line.split(',')
    return (arr[0], arr[2], int(arr[3]))

lines = sc.textFile("file:///SparkCourse/1800.csv")
rdd = lines.map(get_req_columns)
rdd = rdd.filter(lambda x: 'TMAX' in x[1])
rdd = rdd.map(lambda x: (x[0], x[2]))
rdd = rdd.reduceByKey(lambda x, y: max(x,y))
result = rdd.collect()

for tup in result:
    print('{},{:.2f}F'.format(tup[0], tup[1] * 0.1 * (9.0/5.0) + 32))
