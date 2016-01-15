/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Ember from 'ember';

export default Ember.Component.extend({

  content: null,

  statusTypes: {
    // Basic types
    "default": "default",
    "primary": "primary",
    "success": "success",
    "info": "info",
    "warning": "warning",
    "danger": "danger",

    // Extended types
    "new": "default",
    "inited": "primary",
    "initializing": "primary",
    "scheduled": "primary",
    "start_wait": "primary",
    "running": "info",
    "succeeded": "success",
    "failed": "warning",
    "fail_in_progress": "warning",
    "killed": "danger",
    "kill_wait": "warning",
    "kill_in_progress": "warning",
    "error": "danger",
    "terminating": "warning",
    "committing": "info",
  },

  statusType: Ember.computed("content", function () {
    var content = this.get("content"),
        statusType;

    if(content) {
      content = content.toString().toLowerCase();
      statusType = this.get(`statusTypes.${content}`) || 'default';
    }

    return statusType;
  })
});
