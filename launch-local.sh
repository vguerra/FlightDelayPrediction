#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

spark-submit --verbose --master local \
    --class FlightProject "${DIR}/target/scala-2.11/flightproject_2.11-1.0.jar"
