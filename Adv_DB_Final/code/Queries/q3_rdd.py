from pyspark.sql import SparkSession
import pydoop.hdfs as hd
from io import StringIO
import csv
import time

spark = SparkSession.builder.appName('q3_rdd').getOrCreate()
sc = spark.sparkContext

start = time.time()

ratings = \
	sc.textFile("hdfs://master:9000/data/ratings.csv"). \
	map(lambda x: (x.split(',')[1], (float(x.split(',')[2]), 1))). \
	reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
	map(lambda x: (x[0], (x[1][0]/x[1][1], x[1][1])))
#Map: (movieID, (rating, 1))
#Reduce: (movieID, (sumRating, countRating))
#Map: (movieID, (avgRating, countRating))
movie_genres = \
	sc.textFile("hdfs://master:9000/data/movie_genres.csv"). \
	map(lambda x: (x.split(',')[0], x.split(',')[1])).distinct()
#Map: (movieID, genre)
#Total: (movieID, (genre, (avgRating, countRating)))
res = movie_genres.join(ratings).\
	map(lambda x: (x[1][0], (float(x[1][1][0]), 1))). \
	reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
	map(lambda x: (x[0], (x[1][0]/x[1][1], x[1][1]))). \
	sortByKey(ascending=True)
#Map: (genre, (avgRating, 1))
#Reduce: (genre, (sum(avgRating), count(avgRating)))
#Map: (genre, (avgRating per genre, countRating per genre))

print("Genre            |Average Rating        |Number of Movies")

for i in res.collect():
	print('{:<17}|'.format(str(i[0])) + '{:<22}'.format(str(i[1][0])) + '|{}'.format(str(i[1][1])))

end = time.time()
print('Completed in ' + str(end-start) + ' seconds')

