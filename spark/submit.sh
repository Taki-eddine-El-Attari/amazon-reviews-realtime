#!/bin/bash

echo "Soumission du job Spark Streaming..."

MSYS_NO_PATHCONV=1 docker exec spark-master \
    sh -lc 'mkdir -p /tmp/checkpoint /tmp/ivy && exec /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf spark.jars.ivy=/tmp/ivy \
    --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoint \
    /opt/spark/work/streaming_predict.py'

echo "Job soumis"