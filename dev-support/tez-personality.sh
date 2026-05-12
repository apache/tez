#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Apache Yetus personality for Apache Tez

personality_plugins "all"

## @description  Globals for this personality
## @audience     private
## @stability    evolving
## @replaceable  no
function personality_globals
{
  # shellcheck disable=SC2034
  PROJECT_NAME=tez
  # shellcheck disable=SC2034
  BUILDTOOL=maven
  # shellcheck disable=SC2034
  PATCH_BRANCH_DEFAULT=master
  # shellcheck disable=SC2034
  JIRA_ISSUE_RE='^TEZ-[0-9]+$'
  # shellcheck disable=SC2034
  GITHUB_REPO="apache/tez"

  # Increase memory for maven
  # shellcheck disable=SC2034
  MAVEN_OPTS="${MAVEN_OPTS:-"-Xmx4g -XX:+UseG1GC"}"

  # Default Yetus settings for Tez
  # shellcheck disable=SC2034
  DOCKERMEMLIMIT=20g
  # shellcheck disable=SC2034
  PROCLIMIT=5500
}

## @description  Parse extra arguments
## @audience     private
## @stability    evolving
## @replaceable  no
function personality_parse_args
{
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--github-write-comment")
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--github-use-emoji-vote")
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--reapermode=kill")
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--sentinel")
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--mvn-custom-repos")
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--skip-dirs=dev-support")
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--tests-filter=checkstyle")
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--archive-list=checkstyle-errors.xml,spotbugsXml.xml")

  # Environment and Paths
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--docker")
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--java-home=/opt/java/openjdk")
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--shelldocs=${BASEDIR}/dev-support/bin/shelldocs")
  # shellcheck disable=SC2034
  YETUS_ARGS+=("--build-url-artifacts=artifact/out")
}

## @description  Module selection
## @audience     private
## @stability    evolving
## @replaceable  no
function personality_modules
{
  local repostatus=$1
  local testtype=$2
  local extra=""
  local MODULES=("${CHANGED_MODULES[@]}")

  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  extra="-Ptest-patch"

  # Always run these tests on the root module to ensure everything is covered
  if [[ ${testtype} == unit ]]; then
    yetus_debug "Forcing root module and fail-at-end for ${testtype}"
    extra="${extra} -fae"
    MODULES=(.)
  elif [[ ${testtype} == compile ]]; then
    yetus_debug "Forcing root module for ${testtype}"
    MODULES=(.)
  fi

  if [[ ${testtype} == spotbugs ]]; then
    extra="${extra} -Pspotbugs"
  fi

  for module in "${MODULES[@]}"; do
    # shellcheck disable=SC2086
    personality_enqueue_module ${module} ${extra}
  done
}
