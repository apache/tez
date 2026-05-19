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
function personality_globals {
  export PROJECT_NAME=tez
  export BUILDTOOL=maven
  export PATCH_BRANCH_DEFAULT=master
  export JIRA_ISSUE_RE='^TEZ-[0-9]+$'
  export GITHUB_REPO="apache/tez"

  # Increase memory for maven
  export MAVEN_OPTS="${MAVEN_OPTS:-"-Xmx4g -XX:+UseG1GC"}"

  # Default Yetus settings
  export DOCKER_MEMORY="20g"
  export PROC_LIMIT=5500

  # Force-enable core verification plugins
  add_test compile
  add_test unit
}

## @description  Module selection
## @audience     private
## @stability    evolving
## @replaceable  no
function personality_modules {
  local repostatus=$1
  local testtype=$2
  local extra=()
  local MODULES=("${CHANGED_MODULES[@]}")

  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  extra=("-Ptools")

  # Apply strict linting profile only during the explicit compile phase.
  # Yetus ignores warnings during mvninstall, and unit tests are too slow with strict linting.
  if [[ ${testtype} == "compile" ]]; then
    extra+=("-Ptest-patch")
  fi

  # Execute core compilation, unit tests, and install globally (project root)
  if [[ ${testtype} == "unit" || ${testtype} == "compile" || ${testtype} == "mvninstall" ]]; then
    yetus_debug "Forcing root module for ${testtype}"
    MODULES=(.)
  fi

  if [[ ${testtype} == "spotbugs" ]]; then
    extra+=("-Pspotbugs")
  fi

  for module in "${MODULES[@]}"; do
    personality_enqueue_module "${module}" "${extra[@]}"
  done
}

## @description  File filter to determine which tests to run
## @audience     private
## @stability    evolving
## @replaceable  no
function personality_file_filter {
  local filename=$1

  yetus_debug "Tez file filter: ${filename}"

  if [[ ${filename} =~ \.java$ || ${filename} =~ \.proto$ || ${filename} =~ pom\.xml$ ]]; then
    add_test javac
    add_test spotbugs
    add_test checkstyle
    add_test javadoc
  fi

  if [[ ${filename} =~ findbugs-exclude\.xml$ ]]; then
    add_test spotbugs
  fi

  if [[ ${filename} =~ \.sh$ ]] || [[ ${filename} =~ Jenkinsfile ]]; then
    add_test shellcheck
  fi

  if [[ ${filename} =~ \.md$ ]] || [[ ${filename} =~ \.txt$ ]]; then
    add_test codespell
  fi
}
