@echo off

set LINAGE=%1
set ENVIRONMENT=%2

if "%LINAGE%" == "true" (
  echo "Running docker-compose"
  docker-compose up -d
  echo "Running main.py"
  spark-submit --packages za.co.absa.spline.agent.spark:spark-3.1-spline-agent-bundle_2.12:1.0.2 --conf "spark.sql.queryExecutionListeners=za.co.absa.spline.harvester.listener.SplineQueryExecutionListener" --conf "spark.spline.producer.url=http://localhost:8080/producer" ./src/main.py
) else (
  echo "Running other command"
  python src/main.py
)