from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("word-count-my-version")
sc = SparkContext(conf = conf)

def parseLines(line):
    global pat_obj
    return re.findall(pat_obj, line.lower())
    
lines = sc.textFile("file:///SparkCourse/book.txt")
pat_obj = re.compile(r'\w+', re.UNICODE)
words = lines.flatMap(parseLines)

result = words.countByValue()

for word in sorted(result.keys(), key = lambda x: result[x], reverse=True):
    print('{} - {}'.format(word,result[word]))
