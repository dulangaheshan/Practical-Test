#!/bin/bash

leanage=$1
environment=$2

if [ "$leanage" = true ]; then
    docker-compose up -d
    spark-submit --packages za.co.absa.spline.agent.spark:spark-3.1-spline-agent-bundle_2.12:1.0.2 --conf "spark.sql.queryExecutionListeners=za.co.absa.spline.harvester.listener.SplineQueryExecutionListener" --conf "spark.spline.producer.url=http://localhost:8080/producer" ./src/main.py
else
    python src/main.py $environment
fi