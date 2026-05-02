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

set -xeou pipefail

#############################################
# Mocking DAGAppMaster#main() env variables #
#############################################

: "${USER:="tez"}"
: "${LOCAL_DIRS:="/tmp"}"
: "${LOG_DIRS:="/opt/tez/logs"}"
: "${APP_SUBMIT_TIME_ENV:=$(($(date +%s) * 1000))}"
: "${TEZ_AM_EXTERNAL_ID:="tez-session-$(hostname)"}"

export USER LOCAL_DIRS LOG_DIRS APP_SUBMIT_TIME_ENV TEZ_AM_EXTERNAL_ID

mkdir -p "$LOG_DIRS"

###########################
# Custom Config directory #
###########################
if [[ -n "${TEZ_CUSTOM_CONF_DIR:-}" ]] && [[ -d "$TEZ_CUSTOM_CONF_DIR" ]]; then
    echo "--> Using custom configuration directory: $TEZ_CUSTOM_CONF_DIR"
    find "${TEZ_CUSTOM_CONF_DIR}" -type f -exec \
        ln -sf {} "${TEZ_CONF_DIR}"/ \;

    # Remove template keyword if it exists
    if [[ -f "$TEZ_CONF_DIR/tez-site.xml.template" ]]; then
        envsubst < "$TEZ_CONF_DIR/tez-site.xml.template" > "$TEZ_CONF_DIR/tez-site.xml"
    fi
fi

#############
# CLASSPATH #
#############

# Order is: conf -> plugins -> tez jars
CLASSPATH="${TEZ_CONF_DIR}"

# Custom Plugins
# This allows mounting a volume at /opt/tez/plugins containing aux jars
PLUGIN_DIR="/opt/tez/plugins"
if [[ -d "$PLUGIN_DIR" ]]; then
    count=$(find "$PLUGIN_DIR" -maxdepth 1 -name "*.jar" 2>/dev/null | wc -l)
    if [ "$count" != "0" ]; then
        echo "--> Found $count plugin jars. Prepending to classpath."
        CLASSPATH="${CLASSPATH}:${PLUGIN_DIR}/*"
    fi
fi

# Tez Jars
CLASSPATH="${CLASSPATH}:${TEZ_HOME}/*:${TEZ_HOME}/lib/*"

#############
# Execution #
#############

echo "--> Starting DAGAppMaster..."

: "${TEZ_AM_HEAP_OPTS:="-Xmx2048m"}"
# : "${TEZ_AM_GC_OPTS:="-Xlog:gc*=info,class+load=info::time,uptime,level,tags -XX:+UseNUMA"}"

JAVA_ADD_OPENS=(
    "--add-opens=java.base/java.lang=ALL-UNNAMED"
    "--add-opens=java.base/java.util=ALL-UNNAMED"
    "--add-opens=java.base/java.io=ALL-UNNAMED"
    "--add-opens=java.base/java.net=ALL-UNNAMED"
    "--add-opens=java.base/java.nio=ALL-UNNAMED"
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED"
    "--add-opens=java.base/java.util.regex=ALL-UNNAMED"
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED"
    "--add-opens=java.sql/java.sql=ALL-UNNAMED"
    "--add-opens=java.base/java.text=ALL-UNNAMED"
    "-Dnet.bytebuddy.experimental=true"
)

read -r -a JAVA_OPTS_ARR <<< "${JAVA_OPTS:-}"
read -r -a HEAP_OPTS_ARR <<< "${TEZ_AM_HEAP_OPTS}"
# read -r -a JAVA_GC_OPTS_ARR <<< "${TEZ_AM_GC_OPTS}"

# Add "${JAVA_GC_OPTS_ARR[@]}" in following command to get gc information.
exec java "${HEAP_OPTS_ARR[@]}" "${JAVA_OPTS_ARR[@]}" "${JAVA_ADD_OPENS[@]}" \
    -Djava.net.preferIPv4Stack=true \
    -Djava.io.tmpdir="$PWD/tmp" \
    -Dtez.root.logger=INFO,CLA,console \
    -Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator \
    -Dlog4j.configuration=tez-container-log4j.properties \
    -Dyarn.app.container.log.dir="$LOG_DIRS" \
    -Dtez.conf.dir="$TEZ_CONF_DIR" \
    -cp "$CLASSPATH" \
    org.apache.tez.dag.app.DAGAppMaster --session \
    "$@"
