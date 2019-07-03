from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Amt-Spent-by-Customer")
sc = SparkContext(conf = conf)

def get_cust_amt(line):
    arr = line.split(',')
    return (int(arr[0]), float(arr[2]))
    
lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
custamts = lines.map(get_cust_amt)
custamts = custamts.reduceByKey(lambda x,y: x+y)
amtscust_sorted = custamts.map(lambda x: (x[1],x[0])).sortByKey()
result = amtscust_sorted.collect()

for tup in result:
    print('{} - {:.2f}'.format(tup[1],tup[0]))
