from pyspark.sql import SparkSession
import pydoop.hdfs as hd
from io import StringIO
import csv
import time 

spark = SparkSession.builder.appName('q4_rdd').getOrCreate()
sc = spark.sparkContext

def reader(x):
	return list(csv.reader(StringIO(x), delimiter=','))[0]

start = time.time()

movie_sums = \
	sc.textFile("hdfs://master:9000/data/movies.csv"). \
	map(lambda x: reader(x)). \
	filter(lambda x: (x[3]!="" and 2000<=int(x[3].split('-')[0]) and int(x[3].split('-')[0])<=2019)). \
	flatMap(lambda x: ([((x[0], int(x[3].split('-')[0])), 1) for i in x[2].split(' ')])). \
	reduceByKey(lambda x, y: x+y). \
	map(lambda x: (x[0][0], (x[0][1], x[1])))
#Map: (movieID, (year, summary))
#Map: ((movieID, year), 1)
#Reduce: (movieID, (year, #of words))

movie_genres = \
	sc.textFile("hdfs://master:9000/data/movie_genres.csv"). \
	map(lambda x: (x.split(',')[0], x.split(',')[1])).distinct(). \
	filter(lambda x: x[1] == 'Drama')
#Map: (movieID, genre)

#(movieID, ((year, #ofwords), genres))-->(yearPeriod, (#ofwords, 1))
res = movie_sums.join(movie_genres).\
	map(lambda x: ("2000-2004" if 2000<=x[1][0][0]<=2004 
		else ("2005-2009" if 2005<=x[1][0][0]<=2009 
		else ("2010-2014" if 2010<=x[1][0][0]<=2014 else "2015-2019")), (x[1][0][1], 1))). \
	reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
	map(lambda x: (x[0], (x[1][0]/x[1][1]))). \
	sortByKey(ascending=True)
#Map: (period, (#ofWords, 1))
#Reduce: (period, (sumofWords, countofRatings))
#Map: (period, AvgRatingLength)

print('5 Year Period|Average Length')
for i in res.collect():
	print('{:<13}|'.format(str(i[0])) + str(i[1]))


end = time.time()
print('Completed in ' + str(end-start) + ' seconds')
