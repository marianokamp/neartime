#/bin/sh
docker exec -it spark-driver \
         spark-submit \
           --name Test \
           --master spark://master:7077 \
           --class test.TestApp \
           /code/scala-2.11/e2e_2.11-1.0.jar