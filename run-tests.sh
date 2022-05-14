#!/bin/bash

set -euo pipefail

cd "$(dirname "$0")"

trap "docker-compose --file docker-compose.test.yaml down" EXIT

docker-compose --file docker-compose.test.yaml run test