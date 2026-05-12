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
  if [[ ${testtype} == unit || ${testtype} == compile || ${testtype} == mvninstall ]]; then
    yetus_debug "Forcing root module and fail-at-end for ${testtype}"
    extra="${extra} -fae"
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

## @description  File filter to determine which tests to run
## @audience     private
## @stability    evolving
## @replaceable  no
function tez_file_filter
{
  local filename=$1

  yetus_debug "Tez file filter: ${filename}"

  # Unconditionally add compile and unit tests to ensure full build/UT on every PR as requested
  add_test compile
  add_test unit

  if [[ ${filename} =~ \.java$ ]]; then
    add_test javac
    add_test spotbugs
    add_test checkstyle
  fi

  if [[ ${filename} =~ \.sh$ ]] || [[ ${filename} =~ Jenkinsfile ]]; then
    add_test shellcheck
  fi

  if [[ ${filename} =~ \.md$ ]] || [[ ${filename} =~ \.txt$ ]]; then
    add_test codespell
  fi
}
