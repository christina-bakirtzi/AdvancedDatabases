
from pyspark.sql import SparkSession
import time

start = time.time()
spark = SparkSession.builder.appName("query4-sql-b").getOrCreate()
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )

movies = spark.read.parquet("hdfs://master:9000/data/movies.parquet")
movie_genres = spark.read.parquet("hdfs://master:9000/data/movie_genres.parquet")
def format_name(name):
        fName = name.split(" ")[0]
        lName = name.split(" ")[1]
        return lName + " " + fName[0] + "."

movies.registerTempTable("movies")
spark.udf.register("formatter", format_name)

movie_genres.registerTempTable("movie_genres")
spark.udf.register("formatter", format_name)

sqlString = \
        "select sum(length(movies._c2) - length(replace(movies._c2, ' ', ''))+1)/count(movies._c0) as Length, " + \
        "CASE "+\
        "when year(movies._c3) >= 2000 and year(movies._c3)<=2004 then '2000 - 2004' " +\
        "when year(movies._c3) >= 2005 and year(movies._c3)<=2009 then '2005 - 2009' " +\
        "when year(movies._c3) >= 2010 and year(movies._c3)<=2014 then '2010 - 2014' " +\
        "when year(movies._c3) >= 2015 and year(movies._c3)<=2019 then '2015 - 2019' " +\
        "END as Year " + \
        "from movies, movie_genres " +\
        "where movies._c0 = movie_genres._c0 and movie_genres._c1 = 'Drama' and year(movies._c3)>=2000 " + \
        "group by Year " + \
        "order by Year"

res = spark.sql(sqlString)

res.show()
end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')