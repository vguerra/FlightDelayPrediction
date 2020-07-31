#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

spark-submit --verbose --master yarn \
    --conf spark.driver.cores=12 \
    --class FlightProject "${DIR}/target/scala-2.11/flightproject_2.11-1.0.jar" \
    "/user/vm.guerramoran/flights_data/"