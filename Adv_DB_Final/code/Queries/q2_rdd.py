from pyspark.sql import SparkSession
import pydoop.hdfs as hd
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName('q2_rdd').getOrCreate()
sc = spark.sparkContext

start = time.time()

res = \
	sc.textFile("hdfs://master:9000/data/ratings.csv"). \
	map(lambda x: (x.split(',')[0], (float(x.split(',')[2]), 1))).\
	reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
	map(lambda x: (1, (1 if float(x[1][0]/x[1][1])>3 else 0, 1))). \
	reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
	map(lambda x: 100*x[1][0]/x[1][1])

#Map: (userID, (rating, 1))
#Reduce: (userID, (sumRating, countRating))
#Map: (1, (bool, 1))
#Reduce: (1, (good, total))
#Map: (1, percentage)

for i in res.collect():
	print('Percentage: ' + str(i))

end = time.time()
print('Completed in ' + str(end-start) + ' seconds')
