#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set -euo pipefail

# Default number of worker nodes to spin up
NUM_WORKERS=${1:-2}

echo "Starting Tez cluster via docker-compose..."

# Run docker-compose up, scaling the tez-worker service to the desired number
docker-compose up -d --scale tez-worker="${NUM_WORKERS}"

echo "Tez cluster started successfully with ${NUM_WORKERS} worker container(s)."
echo "You can check the logs via: docker-compose logs -f tez-am"
