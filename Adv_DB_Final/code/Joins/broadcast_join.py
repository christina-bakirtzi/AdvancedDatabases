

from pyspark.sql import SparkSession
import csv
from io import StringIO
import time 

start_time = time.time()

spark=SparkSession.builder.appName("broadcast_join").getOrCreate()
sc=spark.sparkContext

R_PATH = "hdfs://master:9000/data/movie_genres_100.csv"
L_PATH = "hdfs://master:9000/data/ratings.csv"



def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

#takes a key-value pair from L-data and searches the HashMap using that key. Makes tuples of (key, (r,val)) for every r-value stored in Hashmap under that key.
def combines(key, val):
        combined = []
        if BrMap.value.get(key, "-") == "-":
                return combined
        for r in BrMap.value.get(key):
                combined.append((key, (r, val)))
        return combined

#reads input file rows, makes (key, (values)) pairs, makes a list of the values for each key and creates a HashMap.
broadcast_data = sc.textFile(R_PATH). \
        map(lambda row: split_complex(row)). \
        map(lambda x : (x[0], (x[1]) ) ). \
        groupByKey(). \
        map(lambda x : (x[0], list(x[1]))). \
        collectAsMap()

#Broadcasting the small dataset
BrMap = sc.broadcast(broadcast_data)

#reads input file rows, makes (key,(values)) pairs, uses the "combines" function on them, adding each returned result to one list as join operation would.
result = \
        sc.textFile(L_PATH). \
        map(lambda row: split_complex(row)). \
        map(lambda x: ( x[1], (x[0], x[2], x[3]) )). \
        flatMap(lambda x: combines(x[0],x[1]))

print(result.collect())
print("Total time: {} sec".format(time.time()-start_time))
