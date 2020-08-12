#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

spark-submit --verbose --master yarn \
    --conf spark.driver.cores=12 \
    --conf spark.executor.cores=12 \
    --conf spark.yarn.submit.waitAppCompletion=true \
    --num-executors 100 \
    --executor-memory 16g \
    --class FlightProject "${DIR}/target/scala-2.11/FlightProject-assembly-1.0.jar" \
    --dataDir "/user/vm.guerramoran/flights_data/" \
    --outputDir "/user/vm.guerramoran/flights_data/output/"
