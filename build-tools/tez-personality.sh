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

personality_plugins "all"

## @description  Globals specific to this personality
## @audience     private
## @stability    evolving
function personality_globals
{
  # shellcheck disable=SC2034
  BUILDTOOL=maven
  #shellcheck disable=SC2034
  PATCH_BRANCH_DEFAULT=master
  #shellcheck disable=SC2034
  JIRA_ISSUE_RE='^TEZ-[0-9]+$'
  #shellcheck disable=SC2034
  GITHUB_REPO_DEFAULT="apache/tez"
  #shellcheck disable=SC2034
  PATCH_NAMING_RULE="https://cwiki.apache.org/confluence/display/TEZ/How+to+Contribute+to+Tez"
}

## @description  maven personality handler
## @audience     private
## @stability    evolving
## @replaceable  yes
function maven_builtin_personality_modules
{
  declare repostatus=$1
  declare testtype=$2
  declare module

  yetus_debug "Using custom Tez personality_modules"
  yetus_debug "Personality: ${repostatus} ${testtype}"

  clear_personality_queue

  # this always makes sure the local repo has a fresh
  # copy of everything per pom rules.
  if [[ ${repostatus} == branch
        && ${testtype} == mvninstall ]] ||
     [[ "${BUILDMODE}" = full ]];then
    personality_enqueue_module .
    return
  fi

  # force running all unit tests if we are doing a Tez precommit
  if [[ ${repostatus} == patch
        && ${testtype} == unit ]];then
    personality_enqueue_module .
    return
  fi

  for module in "${CHANGED_MODULES[@]}"; do
    personality_enqueue_module "${module}"
  done
}
