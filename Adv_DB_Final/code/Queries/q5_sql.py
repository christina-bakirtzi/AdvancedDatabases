from pyspark.sql import SparkSession
import time

start = time.time()

spark = SparkSession.builder.appName("query5-sql-a").getOrCreate()
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )

movies = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/data/movies.csv")
movie_genres = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/data/movie_genres.csv")
ratings = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/data/ratings.csv")
def format_name(name):
        fName = name.split(" ")[0]
        lName = name.split(" ")[1]
        return lName + " " + fName[0] + "."

movies.registerTempTable("movies")
spark.udf.register("formatter", format_name)

movie_genres.registerTempTable("movie_genres")
spark.udf.register("formatter", format_name)

ratings.registerTempTable("ratings")
spark.udf.register("formatter", format_name)

sqlString = \
        "select Table2.Genre as Genre, first(Table2.User) as User, first(Table2.Ratings) as Ratings, first(Table1.movie) as Favorite_Movie, first(max_rating) as Max_Rating, first(Table3.movie) as Least_Favorite_Movie, first(min_rating) as Min_Rating " +\
        "from ( " +\
        "select CountTable2.Genre as Genre, CountTable2.user as User, max_count as Ratings, max_rating, min_rating " +\
        "from (select max(Rating_count) as max_count, Genre " +\
        "from (select ratings._c0 as user,movie_genres._c1 as Genre, count(ratings._c0) as Rating_count " +\
        "from ratings, movie_genres "+\
        "where ratings._c1 = movie_genres._c0 "+\
        "group by movie_genres._c1, ratings._c0) as CountTable "+\
        "group by Genre) as MaxTable, (select ratings._c0 as user, movie_genres._c1 as Genre, count(ratings._c0) as Rating_count, max(ratings._c2) as max_rating, min(ratings._c2) as min_rating " +\
        "from ratings, movie_genres "+\
        "where ratings._c1 = movie_genres._c0 "+\
        "group by movie_genres._c1, ratings._c0) as CountTable2 " +\
        "where CountTable2.Rating_count=MaxTable.max_count and CountTable2.Genre = MaxTable.Genre "+\
        "order by Genre asc) as Table2, "+\
        "(select ratings._c0 as user, ratings._c1 as id, movie_genres._c1 as genre, movies._c1 as movie, ratings._c2 as rating, movies._c7 as popularity " +\
        "from ratings, movies, movie_genres " +\
        "where ratings._c1 = movies._c0 and movies._c0 = movie_genres._c0 " +\
        "order by user desc, rating desc, popularity desc) as Table1, " +\
        "(select ratings._c0 as user, ratings._c1 as id, movie_genres._c1 as genre, movies._c1 as movie, ratings._c2 as rating, movies._c7 as popularity " +\
        "from ratings, movies, movie_genres " +\
        "where ratings._c1 = movies._c0 and movies._c0 = movie_genres._c0 " +\
        "order by user desc, rating desc, popularity desc) as Table3 " +\
        "where Table2.User = Table1.user and Table2.User = Table3.user and Table2.max_rating = Table1.rating and Table2.min_rating = Table3.rating and Table2.Genre = Table1.genre and Table2.Genre = Table3.genre " +\
        "group by Table2.Genre "+\
        "order by Table2.Genre asc"

res = spark.sql(sqlString)

res.show()
end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')

# 1os: user| Genre|Rating_count: counttable1
# 2os: max_count|Genre grouped by genre: MaxTable
# 3os: user| Genre|Rating_count|max_rating|min_rating: count table2

#select maxrc by genre, combine me to idio table
