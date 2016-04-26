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

var processIndex = 1;

export default Ember.Object.extend({
  _id: null,

  name: null,
  events: [], // An array of objects with name and time as mandatory(else error) properties.
  eventBars: null,

  index: 0,
  color: null,

  blockers: null, // Array of processes that's blocking the current process
  blocking: null, // Array of processes blocked by the current process

  blockingEventName: null,

  consolidateStartTime: Ember.computed.oneWay("startEvent.time"),
  consolidateEndTime: Ember.computed.oneWay("endEvent.time"),

  init: function () {
    this.set("_id", `process-id-${processIndex}`);
    processIndex++;
  },

  getColor: function (lightnessFactor) {
    var color = this.get("color"),
        l;

    if(!color) {
      return "#0";
    }
    l = color.l;
    if(lightnessFactor !== undefined) {
      l += 5 + 25 * lightnessFactor;
    }
    return `hsl( ${color.h}, ${color.s}%, ${l}% )`;
  },

  getBarColor: function (barIndex) {
    return this.getColor(1 - (barIndex / this.get("eventBars.length")));
  },

  getConsolidateColor: function () {
    return this.getColor();
  },

  startEvent: Ember.computed("events.@each.time", function () {
    var events = this.get("events"),
        startEvent;
    if(events) {
      startEvent = events[0];
        events.forEach(function (event) {
          if(startEvent.time > event.time) {
            startEvent = event;
          }
      });
    }
    return startEvent;
  }),

  endEvent: Ember.computed("events.@each.time", function () {
    var events = this.get("events"),
        endEvent;
    if(events) {
      endEvent = events[events.length - 1];
      events.forEach(function (event) {
        if(endEvent.time < event.time) {
          endEvent = event;
        }
      });
    }
    return endEvent;
  }),

  getAllBlockers: function (parentHash) {
    var blockers = [],
        currentId = this.get("_id");

    parentHash = parentHash || {}; // To keep a check on cyclic blockers

    parentHash[currentId] = true;
    if(this.get("blockers.length")) {
      this.get("blockers").forEach(function (blocker) {
        if(!parentHash[blocker.get("_id")]) {
          blockers.push(blocker);
          blockers.push.apply(blockers, blocker.getAllBlockers(parentHash));
        }
      });
    }
    parentHash[currentId] = false;

    return blockers;
  },

  getTooltipContents: function (type/*, options*/) {
    return [{
      title: this.get("name"),
      description: "Mouse on : " + type
    }];
  }

});
