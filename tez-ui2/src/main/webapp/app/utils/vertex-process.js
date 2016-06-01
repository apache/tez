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

  edgeHash: null,

  eventBars: [{
    fromEvent: "FIRST_TASK_STARTED",
    toEvent: "LAST_TASK_FINISHED",
  }, {
    fromEvent: "DEPENDENT_VERTICES_COMPLETE",
    toEvent: "LAST_TASK_FINISHED",
  }],

  init: function () {
    this._super();
    this.set("edgeHash", Ember.Object.create());
  },

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
    "vertex.firstTaskStartTime", "vertex.lastTaskFinishTime", "unblockDetails",
    function () {
      var events = [],
          eventsHash = this.get("eventsHash"),

          initTime = this.get("vertex.initTime"),
          startTime = this.get("vertex.startTime"),
          endTime = this.get("vertex.endTime"),

          firstTaskStartTime = this.get("vertex.firstTaskStartTime"),
          lastTaskFinishTime = this.get("vertex.lastTaskFinishTime"),
          unblockDetails = this.get("unblockDetails");

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
        eventsHash["FIRST_TASK_STARTED"] = {
          name: "FIRST_TASK_STARTED",
          time: firstTaskStartTime
        };
      }

      if(unblockDetails && unblockDetails.time >= firstTaskStartTime) {
        eventsHash["DEPENDENT_VERTICES_COMPLETE"] = {
          name: "DEPENDENT_VERTICES_COMPLETE",
          time: unblockDetails.time,
          edge: unblockDetails.edge
        };
      }

      if(lastTaskFinishTime > 0) {
        eventsHash["LAST_TASK_FINISHED"] = {
          name: "LAST_TASK_FINISHED",
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

  unblockDetails: Ember.computed("blockers.@each.completeTime", function () {
    var blockers = this.get("blockers"),
        data = {
          time: 0
        };

    if(blockers) {
      blockers.every(function (currentBlocker) {
        var blockerComplete = currentBlocker.get("completeTime");

        if(!blockerComplete) {
          this.blocker = undefined;
          return false;
        }
        else if(blockerComplete > this.time) {
          this.blocker = currentBlocker;
          this.time = blockerComplete;
        }

        return true;
      }, data);
    }

    if(data.blocker) {
      return {
        time: data.blocker.get("completeTime"),
        edge: this.get("edgeHash").get(data.blocker.get("name"))
      };
    }
  }),

  getTipProperties: function (propHash, propArray) {
    propArray = propArray || [];

    MoreObject.forEach(propHash, function (key, value) {
      if(MoreObject.isString(value)) {
        propArray.push({
          name: key,
          value: value,
        });
      }
      else if (MoreObject.isNumber(value)) {
        propArray.push({
          name: key,
          value: value,
          type: "number"
        });
      }
    });

    return propArray;
  },

  getTooltipContents: function (type, options) {
    var contents,
        that = this,
        vertexDescription;

    switch(type) {
      case "consolidated-process":
        vertexDescription = `Contribution ${options.contribution}%`;
        /* falls through */
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
      case "event-bar":
      case "process-line":
        contents = [{
          title: this.get("name"),
        }];
      break;
      case "event":
        var edge;
        contents = options.events.map(function (event) {
          var properties = [{
            name: "Time",
            value: event.time,
            type: "date"
          }];

          if(event.edge) {
            edge = event.edge;
          }
          if(event.info) {
            properties = this.getTipProperties(event.info, properties);
          }

          return {
            title: event.name,
            properties: properties
          };
        }, this);

        if(edge) {
          let sourceClass = edge.edgeSourceClass || "",
              destClass = edge.edgeDestinationClass || "";

          contents.push({
            title: "Edge From Final Dependent Vertex",
            properties: this.getTipProperties({
              "Input Vertex": edge.inputVertexName,
              "Output Vertex": edge.outputVertexName,
              "Data Movement": edge.dataMovementType,
              "Data Source": edge.dataSourceType,
              "Scheduling": edge.schedulingType,
              "Source Class": sourceClass.substr(sourceClass.lastIndexOf(".") + 1),
              "Destination Class": destClass.substr(destClass.lastIndexOf(".") + 1),
            })
          });
        }
      break;
    }

    return contents;
  },

  consolidateStartTime: Ember.computed("vertex.firstTaskStartTime",
      "unblockDetails.time", function () {
        return Math.max(
          this.get("vertex.firstTaskStartTime") || 0,
          this.get("unblockDetails.time") || 0
        );
  }),
  consolidateEndTime: Ember.computed.oneWay("vertex.endTime"),

  getConsolidateColor: function () {
    return this.getBarColor(this.get("unblockDetails") ? 1 : 0);
  },

});
