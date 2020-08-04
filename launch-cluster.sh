#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

spark-submit --verbose --master yarn \
    --conf spark.driver.cores=12 \
    --num-executors 100 \
    --executor-memory 16g \
    --class FlightProject "${DIR}/target/scala-2.11/flightproject_2.11-1.0.jar" \
    --dataDir "/user/vm.guerramoran/flights_data/" \
    --year "{2009,2010,2011,2012,2013}" \
    --month "*"
