/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import Ember from 'ember';

import Process from './process';

const EVENT_TEXTS = {
  VERTEX_INITIALIZED: "Initialized",
  VERTEX_STARTED: "Started",
  VERTEX_FINISHED: "Finished",
  VERTEX_TASK_START: "First Task Start",
  VERTEX_TASK_FINISH: "All Tasks Complete",
  BLOCKING_VERTICES_COMPLETE: "All Blocking Vertices Complete"
};

export default Process.extend({
  vertex: null,

  name: Ember.computed.oneWay("vertex.name"),
  completeTime: Ember.computed.oneWay("vertex.endTime"),

  blockingEventName: "VERTEX_FINISHED",

  events: Ember.computed(
    "vertex.events.@each.timestamp",
    "vertex.firstTaskStartTime",
    "vertex.lastTaskFinishTime",
    "unblockTime",
    function () {
      var events = this.get("vertex.events").map(function (event) {
        return {
          name: event.eventtype,
          test: EVENT_TEXTS[event.eventtype],
          time: event.timestamp
        };
      }),
      firstTaskStartTime = this.get("vertex.firstTaskStartTime"),
      lastTaskFinishTime = this.get("vertex.lastTaskFinishTime"),
      unblockTime = this.get("unblockTime");

      if(firstTaskStartTime) {
        let type = "VERTEX_TASK_START";
        events.push({
          name: type,
          test: EVENT_TEXTS[type],
          time: firstTaskStartTime
        });
      }

      if(lastTaskFinishTime) {
        let type = "VERTEX_TASK_FINISH";
        events.push({
          name: type,
          test: EVENT_TEXTS[type],
          time: lastTaskFinishTime
        });
      }

      if(unblockTime && unblockTime >= firstTaskStartTime) {
        let type = "BLOCKING_VERTICES_COMPLETE";
        events.push({
          name: type,
          test: EVENT_TEXTS[type],
          time: unblockTime
        });
      }

      return events;
    }
  ),

  unblockTime: Ember.computed("blockers.@each.completeTime", function () {
    var blockers = this.get("blockers"),
        time;

    if(blockers) {
      time = 0;
      for(var i = 0, length = blockers.length; i < length; i++) {
        let blockerComplete = blockers[i].get("completeTime");

        if(!blockerComplete) {
          time = undefined;
          break;
        }
        else if(blockerComplete > time) {
          time = blockerComplete;
        }
      }
    }

    return time;
  }),

});
