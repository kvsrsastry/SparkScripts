from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("word-count-better-sorted-rdd-my-version")
sc = SparkContext(conf = conf)

def parseLines(line):
    global pat_obj
    return re.findall(pat_obj, line.lower())

lines = sc.textFile("file:///SparkCourse/Book.txt")
pat_obj = re.compile(r'\w+', re.UNICODE)
words = lines.flatMap(parseLines)
word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
count_word = word_counts.map(lambda x: (x[1], x[0])).sortByKey()
sorted_result = count_word.collect()

for count, word in sorted_result:
    clean_word = word.encode('ascii', 'ignore')
    if clean_word:
        print('{} - {}'.format(clean_word.decode('ascii'), count))
