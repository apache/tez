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

################################################
# 1. Mocking DAGAppMaster#main() env variables #
################################################

: "${CONTAINER_ID:="container_1700000000000_0001_01_000001"}"
: "${USER:="tez"}"
: "${HADOOP_USER_NAME:="tez"}"
: "${NM_HOST:="localhost"}"
: "${NM_PORT:="12345"}"
: "${NM_HTTP_PORT:="8042"}"
: "${LOCAL_DIRS:="/tmp"}"
: "${LOG_DIRS:="/opt/tez/logs"}"
: "${APP_SUBMIT_TIME_ENV:=$(($(date +%s) * 1000))}"
: "${TEZ_AM_EXTERNAL_ID:="tez-session-$(hostname)"}"

export CONTAINER_ID USER HADOOP_USER_NAME NM_HOST NM_PORT NM_HTTP_PORT \
    LOCAL_DIRS LOG_DIRS APP_SUBMIT_TIME_ENV TEZ_AM_EXTERNAL_ID

mkdir -p "$LOG_DIRS"

##########################
# CONFIGURATION HANDLING #
##########################

# Symlink hadoop conf in tez conf dir
if [[ -d "$HADOOP_HOME/etc/hadoop" ]]; then
    echo "--> Linking missing Hadoop configs to $TEZ_CONF_DIR..."
    for f in "$HADOOP_HOME/etc/hadoop"/*; do
        basename=$(basename "$f")
        # this check helps in case user wants to provide its custom hfds-site.xml
        # or any other configuration file
        if [[ ! -e "$TEZ_CONF_DIR/$basename" ]]; then
            ln -s "$f" "$TEZ_CONF_DIR/$basename"
        fi
    done
fi

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

export HADOOP_USER_CLASSPATH_FIRST=true
# Order is: conf -> plugins -> tez jars -> hadoop jars
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

# Hadoop Jars
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/common/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/common/lib/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/hdfs/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/hdfs/lib/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/yarn/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/yarn/lib/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/mapreduce/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/mapreduce/lib/*"

#############
# Execution #
#############
TEZ_DAG_JAR=$(find "$TEZ_HOME" -maxdepth 1 -name "tez-dag-*.jar" ! -name "*-tests.jar" | head -n 1)

if [ -z "$TEZ_DAG_JAR" ]; then
    echo "Error: Could not find tez-dag-*.jar in $TEZ_HOME"
    exit 1
fi

echo "--> Starting DAGAppMaster..."
echo "--> HADOOP_CONF_DIR: $HADOOP_CONF_DIR"

: "${TEZ_AM_HEAP_OPTS:="-Xmx2048m"}"

# Check for Log4j2 Configuration
LOG4J2_FILE="$TEZ_CONF_DIR/log4j2.properties"
if [[ -f "$LOG4J2_FILE" ]]; then
    echo "--> [TEZ-AM] Found Log4j2 configuration: $LOG4J2_FILE"
    JAVA_OPTS="${JAVA_OPTS:+$JAVA_OPTS }-Dlog4j.configurationFile=file:$LOG4J2_FILE"
fi

JAVA_ADD_OPENS=(
    "--add-opens=java.base/java.lang=ALL-UNNAMED"
    "--add-opens=java.base/java.util=ALL-UNNAMED"
    "--add-opens=java.base/java.io=ALL-UNNAMED"
)

read -r -a JAVA_OPTS_ARR <<< "${JAVA_OPTS:-}"
read -r -a HEAP_OPTS_ARR <<< "${TEZ_AM_HEAP_OPTS}"

exec java "${HEAP_OPTS_ARR[@]}" "${JAVA_OPTS_ARR[@]}" "${JAVA_ADD_OPENS[@]}" \
    -Duser.name="$HADOOP_USER_NAME" \
    -Djava.library.path="$HADOOP_HOME/lib/native" \
    -Dhadoop.home.dir="$HADOOP_HOME" \
    -Dhadoop.log.dir="$LOG_DIRS" \
    -Dtez.conf.dir="$TEZ_CONF_DIR" \
    -cp "$CLASSPATH" \
    org.apache.tez.dag.app.DAGAppMaster --session \
    "$@"
