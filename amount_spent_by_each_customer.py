from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Amt-Spent-by-Customer")
sc = SparkContext(conf = conf)

def get_cust_amt(line):
    arr = line.split(',')
    return (int(arr[0]), float(arr[2]))
    
lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
custamts = lines.map(get_cust_amt)
custamts = custamts.reduceByKey(lambda x,y: x+y)
result = custamts.collect()

for tup in result:
    print('{} - {}'.format(tup[0],tup[1]))
