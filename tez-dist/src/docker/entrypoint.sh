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

 #######################
 # 1. PLUGIN EXTENSION #
 #######################
# The directory /opt/tez/plugins is intended to be a volume mount point.
# If custom jars are present, we add them to classpath.

PLUGIN_DIR="/opt/tez/plugins"
PLUGIN_CLASSPATH=""

if [ -d "$PLUGIN_DIR" ]; then
    count=$(find "$PLUGIN_DIR" -maxdepth 1 -name "*.jar" 2>/dev/null | wc -l)
    if [ "$count" != "0" ]; then
        echo "--> Found $count custom jars in $PLUGIN_DIR. Adding to classpath..."
        PLUGIN_CLASSPATH="$PLUGIN_DIR/*"
    else
        echo "--> Plugin directory exists but contains no jars."
    fi
fi

# =========================================================================
# 2. CONFIGURATION HANDLING
# =========================================================================
# 1. Custom Conf Dir: If mounted, symlink it to use it directly.
# 2. Templates: If not custom, use envsubst to generate configs from ENV.

# Point HADOOP_CONF_DIR to TEZ_CONF_DIR, we need to populate it
# with defaults from the Hadoop installation if they aren't provided by the user.
if [ -d "$HADOOP_HOME/etc/hadoop" ]; then
    echo "--> Linking missing Hadoop configs to $TEZ_CONF_DIR..."
    for f in "$HADOOP_HOME/etc/hadoop"/*; do
        basename=$(basename "$f")
        # this check helps in case user wants to provide its custom hfds-site.xml
        # or any other configuration file
        if [ ! -e "$TEZ_CONF_DIR/$basename" ]; then
            ln -s "$f" "$TEZ_CONF_DIR/$basename"
        fi
    done
fi

###########################
# Custom Config directory #
###########################
if [ -n "${TEZ_CUSTOM_CONF_DIR:-}" ] && [ -d "$TEZ_CUSTOM_CONF_DIR" ]; then
    echo "--> Using custom configuration directory: $TEZ_CUSTOM_CONF_DIR"
    find "${TEZ_CUSTOM_CONF_DIR}" -type f -exec \
        ln -sfn {} "${TEZ_CONF_DIR}"/ \;
else
    echo "--> Generating configuration from templates..."
    # Set defaults for template variables if not provided
    export TEZ_AM_RPC_PORT=${TEZ_AM_RPC_PORT:-10001}
    export TEZ_AM_RESOURCE_MEMORY=${TEZ_AM_RESOURCE_MEMORY:-1024}

    # Process templates
    if [ -f "$TEZ_CONF_DIR/tez-site.xml.template" ]; then
        envsubst < "$TEZ_CONF_DIR/tez-site.xml.template" > "$TEZ_CONF_DIR/tez-site.xml"
    fi
fi


####################
# Find TEZ DAG JAR #
####################
TEZ_DAG_JAR=$(find "$TEZ_HOME" -maxdepth 1 -name "tez-dag-*.jar" ! -name "*-tests.jar" | head -n 1)

if [ -z "$TEZ_DAG_JAR" ]; then
    echo "Error: Could not find tez-dag-*.jar in $TEZ_HOME"
    ls -l "$TEZ_HOME"
    exit 1
fi

##############################################
# YARN ENVIRONMENT SIMULATION () #
##############################################
export APP_SUBMIT_TIME_ENV=${APP_SUBMIT_TIME_ENV:-$(($(date +%s) * 1000))}

# 2. Container ID
export CONTAINER_ID=${CONTAINER_ID:-"container_1700000000000_0001_01_000001"}

# 3. NodeManager Details
export NM_HOST=${NM_HOST:-"localhost"}
export NM_PORT=${NM_PORT:-"12345"}
export NM_HTTP_PORT=${NM_HTTP_PORT:-"8042"}
export LOCAL_DIRS=${LOCAL_DIRS:-"/tmp"}
export LOG_DIRS=${LOG_DIRS:-"/opt/tez/logs"}

# 4. User Identity
export HADOOP_USER_NAME=${HADOOP_USER_NAME:-"tez"}
export USER=${HADOOP_USER_NAME}

export TEZ_AM_EXTERNAL_ID=${TEZ_AM_EXTERNAL_ID:-"tez-session-$(hostname)"}

echo "--> Mocked YARN Environment:"
echo "    APP_SUBMIT_TIME_ENV: $APP_SUBMIT_TIME_ENV"
echo "    CONTAINER_ID:        $CONTAINER_ID"
echo "    USER:                $USER"

mkdir -p "$LOG_DIRS"

if [ ! -f "tez-conf.pb" ]; then
    touch "tez-conf.pb"
    echo "--> Created dummy tez-conf.pb"
fi

#############
# EXECUTION #
#############

CLASSPATH="${TEZ_CONF_DIR}:${TEZ_HOME}/*:${TEZ_HOME}/lib/*"

if [ -n "$PLUGIN_CLASSPATH" ]; then
    CLASSPATH="${CLASSPATH}:${PLUGIN_CLASSPATH}"
fi

export HADOOP_USER_CLASSPATH_FIRST=true

CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/common/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/common/lib/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/hdfs/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/hdfs/lib/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/yarn/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/yarn/lib/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/mapreduce/*"
CLASSPATH="${CLASSPATH}:${HADOOP_HOME}/share/hadoop/mapreduce/lib/*"

echo "--> Starting DAGAppMaster with JAR: $TEZ_DAG_JAR"
echo "--> HADOOP_CONF_DIR: $HADOOP_CONF_DIR"

exec java \
    --add-opens java.base/java.lang=ALL-UNNAMED \
    --add-opens java.base/java.util=ALL-UNNAMED \
    --add-opens java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens java.base/java.text=ALL-UNNAMED \
    --add-opens java.base/java.nio=ALL-UNNAMED \
    --add-opens java.base/sun.nio.ch=ALL-UNNAMED \
    --add-opens java.base/java.util.concurrent=ALL-UNNAMED \
    --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED \
    -Duser.name="$HADOOP_USER_NAME" \
    -Djava.library.path="$HADOOP_HOME/lib/native" \
    -Dhadoop.home.dir="$HADOOP_HOME" \
    -Dhadoop.log.dir="$LOG_DIRS" \
    -Dtez.conf.dir="$TEZ_CONF_DIR" \
    -cp "$CLASSPATH" \
    org.apache.tez.dag.app.DAGAppMaster \
    "$@"
