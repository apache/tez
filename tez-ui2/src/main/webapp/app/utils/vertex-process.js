/*global more*/
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

var MoreObject = more.Object;

export default Process.extend({
  vertex: null,

  name: Ember.computed.oneWay("vertex.name"),
  completeTime: Ember.computed.oneWay("vertex.endTime"),

  blockingEventName: "VERTEX_FINISHED",

  getVisibleProps: null,

  eventBars: [{
    fromEvent: "VERTEX_TASK_START",
    toEvent: "VERTEX_TASK_FINISH",
  }, {
    fromEvent: "DEPENDENT_VERTICES_COMPLETE",
    toEvent: "VERTEX_TASK_FINISH",
  }],

  eventsHash: Ember.computed("vertex.events.@each.timestamp", function () {
    var events = {},
        eventsArr = this.get("vertex.events");

    if(eventsArr) {
      eventsArr.forEach(function (event) {
        if(event.timestamp > 0) {
          events[event.eventtype] = {
            name: event.eventtype,
            time: event.timestamp,
            info: event.eventinfo
          };
        }
      });
    }

    return events;
  }),

  events: Ember.computed("eventsHash",
    "vertex.initTime", "vertex.startTime", "vertex.endTime",
    "vertex.firstTaskStartTime", "vertex.lastTaskFinishTime", "unblockTime",
    function () {
      var events = [],
          eventsHash = this.get("eventsHash"),

          initTime = this.get("vertex.initTime"),
          startTime = this.get("vertex.startTime"),
          endTime = this.get("vertex.endTime"),

          firstTaskStartTime = this.get("vertex.firstTaskStartTime"),
          lastTaskFinishTime = this.get("vertex.lastTaskFinishTime"),
          unblockTime = this.get("unblockTime");

      if(initTime > 0) {
        eventsHash["VERTEX_INITIALIZED"] = {
          name: "VERTEX_INITIALIZED",
          time: initTime
        };
      }

      if(startTime > 0) {
        eventsHash["VERTEX_STARTED"] = {
          name: "VERTEX_STARTED",
          time: startTime
        };
      }

      if(firstTaskStartTime > 0) {
        eventsHash["VERTEX_TASK_START"] = {
          name: "VERTEX_TASK_START",
          time: firstTaskStartTime
        };
      }

      if(unblockTime > 0 && unblockTime >= firstTaskStartTime) {
        eventsHash["DEPENDENT_VERTICES_COMPLETE"] = {
          name: "DEPENDENT_VERTICES_COMPLETE",
          time: unblockTime
        };
      }

      if(lastTaskFinishTime > 0) {
        eventsHash["VERTEX_TASK_FINISH"] = {
          name: "VERTEX_TASK_FINISH",
          time: lastTaskFinishTime
        };
      }

      if(endTime > 0) {
        eventsHash["VERTEX_FINISHED"] = {
          name: "VERTEX_FINISHED",
          time: endTime
        };
      }

      MoreObject.forEach(eventsHash, function (key, value) {
        events.push(value);
      });

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

  getTooltipContents: function (type, options) {
    var contents,
        that = this,
        vertexDescription;

    switch(type) {
      case "consolidated-process":
        vertexDescription = `Contribution ${options.contribution}%`;
        /* falls through */
      case "event-bar":
      case "process-line":
      case "process-name":
        let properties = this.getVisibleProps().map(function (definition) {
          return {
            name: definition.get("headerTitle"),
            value: that.get("vertex").get(definition.get("contentPath")),
            type: Ember.get(definition, "cellDefinition.type"),
            format: Ember.get(definition, "cellDefinition.format")
          };
        });

        contents = [{
          title: this.get("name"),
          properties: properties,
          description: vertexDescription
        }];
      break;
      case "event":
        contents = options.events.map(function (event) {
          var properties = [{
            name: "Time",
            value: event.time,
            type: "date"
          }];

          if(event.info) {
            MoreObject.forEach(event.info, function (key, value) {
              if(MoreObject.isString(value)) {
                properties.push({
                  name: key,
                  value: value,
                });
              }
              else if (MoreObject.isNumber(value)) {
                properties.push({
                  name: key,
                  value: value,
                  type: "number"
                });
              }
            });
          }
          return {
            title: event.name,
            properties: properties
          };
        });
      break;
    }

    return contents;
  },

  consolidateStartTime: Ember.computed("vertex.firstTaskStartTime",
      "vertex.unblockTime", function () {
        return Math.max(this.get("vertex.firstTaskStartTime") || 0, this.get("unblockTime") || 0);
  }),
  consolidateEndTime: Ember.computed.oneWay("vertex.endTime"),

  getConsolidateColor: function () {
    return this.getBarColor(this.get("unblockTime") ? 1 : 0);
  },

});
