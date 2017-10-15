#!/bin/sh
         spark-submit \
           --master local[2] \
           --driver-memory 12g \
           --executor-memory 12G \
           --conf spark.network.timeout=10000000 \
           --repositories https://jitpack.io \
           --packages com.github.databricks:spark-avro:204864b6cf\
           --class persistence.PersistenceDemo \
           /Users/mkamp/repos/code/neartime/e2e/target/scala-2.11/e2e_2.11-1.0.jar



