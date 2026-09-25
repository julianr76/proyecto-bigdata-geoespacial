#!/bin/bash
set -e

/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.host=spark-master \
  --conf spark.sql.session.timeZone=UTC \
  --driver-memory 512m \
  --executor-memory 768m \
  "/opt/jobs/$1"
