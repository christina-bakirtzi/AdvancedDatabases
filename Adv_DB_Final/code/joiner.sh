#!/bin/bash

echo "Running SQL join (enabled)"
/bin/bash spark-submit join_sql.py N> outputs/sql_join_N.txt 2> logs/sql_join_N_log.txt

echo "Running SQL join (disabled)"
/bin/bash spark-submit join_sql.py Y> outputs/sql_join_Y.txt 2> logs/sql_join_Y_log.txt

echo "Running broadcast join"
/bin/bash spark-submit broadcast_join.py > outputs/broadcast_join.txt 2> logs/broadcast_join_log.txt

echo "Running repartition join"
/bin/bash spark-submit repartition_join.py > outputs/repartition_join.txt 2> logs/repartition_join_log.txt
