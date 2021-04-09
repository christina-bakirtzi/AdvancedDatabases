from pyspark.sql import SparkSession
import time

start = time.time()
spark = SparkSession.builder.appName("query1-sql-a").getOrCreate()

movies = spark.read.format('csv'). \
                        options(header='false',
                                inferSchema='true'). \
                        load("hdfs://master:9000/data/movies.csv")

movies.registerTempTable("movies")

sqlString = \
        "select (_c1) as Name,year(_c3) as Year, (((movies._c6)-(movies._c5))*100/(movies._c5)) as Profit " + \
        "from movies " + \
        "join "+ \
        "(select year(movies._c3) as Year,  max(((movies._c6)-(movies._c5))*100/(movies._c5)) as Profit " + \
        "from movies "+ \
        "where year(movies._c3)>=2000 and movies._c5>0 and movies._c6>0 "+ \
        "group by year(movies._c3) "+ \
        "order by year(movies._c3) desc " + \
        ") MaxProfit "+ \
        "where MaxProfit.Year = year(movies._c3) and MaxProfit.Profit = ((movies._c6)-(movies._c5))*100/(movies._c5) and movies._c5>0 and movies._c6>0 "+ \
        "order by year(movies._c3) "

res = spark.sql(sqlString)

res.show()
end = time.time()
print('Completed in ' + str(end-start) + ' seconds\n')