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
  PROJECT="tez"
  #shellcheck disable=SC2034
  JIRA_ISSUE_RE='^TEZ-[0-9]+$'
  #shellcheck disable=SC2034
  GITHUB_REPO_DEFAULT="apache/tez"
  #shellcheck disable=SC2034
  PATCH_NAMING_RULE="https://cwiki.apache.org/confluence/display/TEZ/How+to+Contribute+to+Tez"
}
