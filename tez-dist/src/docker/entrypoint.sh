#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -xeou pipefail

: "${TEZ_COMPONENT:="AM"}"

echo "--> Starting Tez Component: $TEZ_COMPONENT"

if [[ "$TEZ_COMPONENT" == "AM" ]]; then
    echo "--> Routing to Tez AM Entrypoint"
    exec /am-entrypoint.sh "$@"
elif [[ "$TEZ_COMPONENT" == "CHILD" ]]; then
    echo "--> Routing to Tez Child Entrypoint"
    exec /child-entrypoint.sh "$@"
else
    echo "Error: Unknown TEZ_COMPONENT '$TEZ_COMPONENT'. Must be 'AM' or 'CHILD'."
    exit 1
fi
