
from pyspark.sql import SparkSession
import time

start = time.time()
spark = SparkSession.builder.appName("query3-sql-b").getOrCreate()
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )

ratings = spark.read.parquet("hdfs://master:9000/data/ratings.parquet")
movie_genres = spark.read.parquet("hdfs://master:9000/data/movie_genres.parquet")
def format_name(name):
        fName = name.split(" ")[0]
        lName = name.split(" ")[1]
        return lName + " " + fName[0] + "."

ratings.registerTempTable("ratings")
spark.udf.register("formatter", format_name)

movie_genres.registerTempTable("movie_genres")
spark.udf.register("formatter", format_name)

sqlString = \
        "select genre as Genre, sum(rating_sum/rating_count)/count(distinct id) as Average_Rating, count(distinct id) as Total_Movies " + \
        "from (select RatingTable.id, rating_sum, rating_count, (movie_genres._c1) as genre " + \
        "from (select _c1 as id, sum(_c2) as rating_sum, count(_c2) as rating_count " + \
        "from ratings " + \
        "group by _c1) as RatingTable, movie_genres " + \
        "where movie_genres._c0 = RatingTable.id) as MixedTable " +\
        "group by genre " +\
        "order by Genre asc"



res = spark.sql(sqlString)

res.show()
end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')