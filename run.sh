#!/bin/bash

linage=$1
environment=$2

if [ "$linage" = true ]; then
    if [[ "$(docker ps --filter name="splinedocker_rest-server" -q)" != "" ]]; then
      echo "Container is already running"
    else
      echo "Container is not running. Starting it up..."
      docker-compose up -d
    fi
    spark-submit --conf spark.ui.retainedJobs=5 --packages za.co.absa.spline.agent.spark:spark-3.1-spline-agent-bundle_2.12:1.0.2 --conf "spark.sql.queryExecutionListeners=za.co.absa.spline.harvester.listener.SplineQueryExecutionListener" --conf "spark.spline.producer.url=http://localhost:8080/producer" ./src/main.py
else
    python src/main.py $environment
fi