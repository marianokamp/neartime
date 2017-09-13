#!/bin/sh
docker exec -it spark-driver \
         spark-submit \
           --total-executor-cores 1\
           --master spark://master:7077 \
           --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1\
           --class $1 \
           /code/scala-2.11/e2e_2.11-1.0.jar \
           $2
