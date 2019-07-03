from pyspark import SparkConf, SparkContext
#import collections

# Create a SparkConf Object to provide it as input to SparkContext Object
# setAppName is to give this application a name to view it in web UI
# setMaster is to specify where to run it - On Local Machine, On Cluster etc.
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

# Create a SparkContext Object with parameter as SparkConf Object
sc = SparkContext(conf = conf)

# Create an RDD (lines) from the SparkContext Object
# Input can be a localfile, hdfs, s3 etc.
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

# RDDs are immutable. We can only create a new RDD out of them (ratings here)
# Call an RDD method (map) to get only the movie ratings
# RDD Methods take FUNCTIONS as input parameters (Functional Programming)
ratings = lines.map(lambda x: x.split()[2])

# Call an ACTION on RDD (countByValue here)
# It returns a python dict object
result = ratings.countByValue()

# Print in Sorted order
for k, v in sorted(result.items()):
    print('{} - {}'.format(k, v))

# Don't need Collections module here. So, commented the code below
#sortedResults = collections.OrderedDict(sorted(result.items()))
#for key, value in sortedResults.items():
#    print("%s %i" % (key, value))
