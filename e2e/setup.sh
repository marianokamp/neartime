#!/bin/sh
docker rm spark-driver
docker run --network=docker_default \
           --name spark-driver \
           --hostname spark-driver\
           --expose=9999\
           --publish=9999:9999\
           -e SPARK_HOME=/usr/local/spark-2.1.1\
           --link docker_master_1 \
           -v /Users/mkamp/repos/code/neartime/e2e/docker/conf/driver:/usr/local/spark-2.1.1/conf \
           -v /Users/mkamp/repos/code/neartime/e2e/target:/code \
           --volumes-from docker_logfiles_1 \
           -d lateralthinking/spark tail -f /dev/null