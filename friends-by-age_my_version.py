#omgamganapathayenamaha
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('avg_friends_by_age')
sc = SparkContext(conf = conf)

lines = sc.textFile("file:///C:\SparkCourse\\fakefriends.csv")
rdd = lines.map(lambda x: (x.split(',')[2], (int(x.split(',')[3]), 1)))
rdd = rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
rdd = rdd.mapValues(lambda x: x[0] / x[1])
result = rdd.collect()

for tup in sorted(result, key=lambda x: x[1], reverse=True):
    print('{} - {}'.format(tup[0], tup[1]))
