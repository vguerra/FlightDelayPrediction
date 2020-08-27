#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

spark-submit --verbose --master yarn \
    --conf spark.driver.cores=12 \
    --conf spark.driver.memory=3g \
    --conf spark.executor.cores=12 \
    --conf spark.executor.memoryOverhead=8g \
    --num-executors 100 \
    --executor-memory 4g \
    --conf spark.yarn.submit.waitAppCompletion=false \
    --class FlightProject "${DIR}/target/scala-2.11/FlightProject-assembly-1.0.jar" \
    --dataDir "viewfs:///user/vm.guerramoran/flights_data/" \
    --outputDir "viewfs:///user/${USER}/flights_output/" \
    --year "{2009,2010,2011,2012,2013}" \
    --month "*" \
    --nbWeatherHours 12
