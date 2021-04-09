#!/bin/bash

for i in 'q1' 'q2' 'q3' 'q4' 'q5';
do
	echo "Running $i in RDD"
	/bin/bash spark-submit "$i"_rdd.py > outputs/"$i"_rdd.txt 2> logs/"$i"_rdd_log.txt
	echo "Running $i in SQL using CSV input"
	/bin/bash spark-submit "$i"_sql.py > outputs/"$i"_sql.txt 2> logs/"$i"_sql_log.txt
	echo "Running $i in SQL using Parquet input"
	/bin/bash spark-submit "$i"_sql_b.py > outputs/"$i"_sql_b.txt 2> logs/"$i"_sql_b_log.txt
done

