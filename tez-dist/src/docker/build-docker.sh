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

HADOOP_VERSION=
TEZ_VERSION=
REPO=

usage() {
  cat <<EOF 1>&2
Usage: $0 [-h] [-hadoop <Hadoop version>] [-tez <Tez version>] [-repo <Docker repo>]
Build the Apache Tez AM Docker image
-help                Display help
-hadoop              Build image with the specified Hadoop version
-tez                 Build image with the specified Tez version
-repo                Docker repository
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
  -h)
    usage
    exit 0
    ;;
  -hadoop)
    shift
    HADOOP_VERSION=$1
    shift
    ;;
  -tez)
    shift
    TEZ_VERSION=$1
    shift
    ;;
  -repo)
    shift
    REPO=$1
    shift
    ;;
  *)
    shift
    ;;
  esac
done

SCRIPT_DIR=$(
  cd "$(dirname "$0")"
  pwd
)

DIST_DIR=${DIST_DIR:-"$SCRIPT_DIR/../.."}
PROJECT_ROOT=${PROJECT_ROOT:-"$SCRIPT_DIR/../../.."}

REPO=${REPO:-apache}
WORK_DIR="$(mktemp -d)"
CACHE_DIR="$SCRIPT_DIR/cache"
mkdir -p "$CACHE_DIR"

# Defaults Hadoop and Tez versions from pom.xml if not provided
HADOOP_VERSION=${HADOOP_VERSION:-$(mvn -f "$PROJECT_ROOT/pom.xml" -q help:evaluate -Dexpression=hadoop.version -DforceStdout)}
TEZ_VERSION=${TEZ_VERSION:-$(mvn -f "$PROJECT_ROOT/pom.xml" -q help:evaluate -Dexpression=project.version -DforceStdout)}

######################
# HADOOP FETCH LOGIC #
######################
HADOOP_FILE_NAME="hadoop-$HADOOP_VERSION.tar.gz"
HADOOP_URL=${HADOOP_URL:-"https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VERSION/$HADOOP_FILE_NAME"}
if [ ! -f "$CACHE_DIR/$HADOOP_FILE_NAME" ]; then
  echo "Downloading Hadoop from $HADOOP_URL..."
  if ! curl --fail -L "$HADOOP_URL" -o "$CACHE_DIR/$HADOOP_FILE_NAME.tmp"; then
    echo "Fail to download Hadoop, exiting...."
    exit 1
  fi
  mv "$CACHE_DIR/$HADOOP_FILE_NAME.tmp" "$CACHE_DIR/$HADOOP_FILE_NAME"
fi

#####################################
# Pick tez tarball from local build #
#####################################
TEZ_FILE_NAME="tez-$TEZ_VERSION.tar.gz"
LOCAL_DIST_PATH="$DIST_DIR/target/$TEZ_FILE_NAME"

if [ -f "$LOCAL_DIST_PATH" ]; then
  echo "--> Found local Tez build artifact at: $LOCAL_DIST_PATH"
  cp "$LOCAL_DIST_PATH" "$WORK_DIR/"
else
  echo "--> Error: Local Tez artifact not found at $LOCAL_DIST_PATH"
  echo "--> Please build the project first (e.g., mvn clean install -DskipTests)."
  exit 1
fi

# -------------------------------------------------------------------------
# BUILD CONTEXT PREPARATION
# -------------------------------------------------------------------------
cp "$CACHE_DIR/$HADOOP_FILE_NAME" "$WORK_DIR/"
cp -R "$SCRIPT_DIR/conf" "$WORK_DIR/" 2>/dev/null || mkdir -p "$WORK_DIR/conf"
cp "$SCRIPT_DIR/tez-am-entrypoint.sh" "$WORK_DIR/"
cp "$SCRIPT_DIR/Dockerfile" "$WORK_DIR/"

echo "Building Docker image..."
docker build \
  "$WORK_DIR" \
  -f "$WORK_DIR/Dockerfile" \
  -t "$REPO/tez-am:$TEZ_VERSION" \
  --build-arg "BUILD_ENV=unarchive" \
  --build-arg "HADOOP_VERSION=$HADOOP_VERSION" \
  --build-arg "TEZ_VERSION=$TEZ_VERSION"

rm -r "${WORK_DIR}"
echo "Docker image $REPO/tez-am:$TEZ_VERSION built successfully."
