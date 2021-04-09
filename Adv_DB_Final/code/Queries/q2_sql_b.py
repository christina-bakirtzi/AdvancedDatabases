from pyspark.sql import SparkSession
import time

start = time.time()
spark = SparkSession.builder.appName("query2-sql-b").getOrCreate()
spark.conf.set( "spark.sql.crossJoin.enabled" , "true" )

ratings = spark.read.parquet("hdfs://master:9000/data/ratings.parquet")
def format_name(name):
        fName = name.split(" ")[0]
        lName = name.split(" ")[1]
        return lName + " " + fName[0] + "."

ratings.registerTempTable("ratings")
spark.udf.register("formatter", format_name)

sqlString = \
        "select (rating_users.users_r)*100/all_users.users_all as Percentage "+\
        "from (select count(distinct _c0) as users_all " + \
        "from ratings " +\
        ")as all_users, " +\
        "(select count(distinct sum_table.id) as users_r " + \
        "from " + \
        "(select (_c0) as id, count(_c3) as number_reviews, sum(_c2) as sum_reviews " + \
        "from ratings " + \
        "group by _c0 " + \
        ") as sum_table " + \
        "where sum_table.sum_reviews/sum_table.number_reviews > 3) as rating_users"


res = spark.sql(sqlString)

res.show()
end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')