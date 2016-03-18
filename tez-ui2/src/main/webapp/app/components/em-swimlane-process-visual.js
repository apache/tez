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
  process: null,
  definition: null,

  startTime: 0,
  endTime: 0,
  timeWindow: 0,

  normalizedEvents: [],
  startEvent: null,
  endEvent: null,

  eventBars: [],
  classNames: ["em-swimlane-process-visual"],

  didInsertElement: function () {
    Ember.run.later(this, "normalizeEvents");
  },

  normalizeEvents: Ember.observer("process.events.@each.time", "startTime", "timeWindow", function () {
    var events = Ember.get(this.get("process"), "events") || [],
        startEvent,
        endEvent,

        startTime = this.get("startTime"),
        timeWindow = this.get("timeWindow");

    events = events.map(function (event) {
      var position = ((event.time - startTime) / timeWindow) * 100;
      event = {
        name: event.name,
        text: event.text || event.name,
        pos: position,
        time: event.time
      };

      if(!startEvent || startEvent.pos > position) {
        startEvent = event;
      }
      if(!endEvent || endEvent.pos < position) {
        endEvent = event;
      }

      return event;
    });

    this.setProperties({
      normalizedEvents: events,
      startEvent: startEvent,
      endEvent: endEvent
    });
  }),

  drawEventWindow: Ember.observer("startEvent.pos", "endEvent.pos", function () {
    this.$(".event-window-line").css({
      left: this.get("startEvent.pos") + "%",
      right: (100 - this.get("endEvent.pos")) + "%",
      "background-color": this.get("process").getColor()
    });
  })

});
