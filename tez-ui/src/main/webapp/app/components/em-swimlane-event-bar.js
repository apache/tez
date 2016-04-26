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

  bar: null,
  barIndex: 0,

  process: null,
  processor: null,

  classNames: ["em-swimlane-event-bar"],

  fromEvent: Ember.computed("process.events.@each.name", "bar.fromEvent", function () {
    var events = this.get("process.events"),
        fromEventName = this.get("bar.fromEvent");
    return events.find(function (event) {
      return event.name === fromEventName;
    });
  }),
  toEvent: Ember.computed("process.events.@each.name", "bar.toEvent", function () {
    var events = this.get("process.events"),
        toEventName = this.get("bar.toEvent");
    return events.find(function (event) {
      return event.name === toEventName;
    });
  }),

  didInsertElement: Ember.observer("fromEvent.time", "toEvent.time",
      "barIndex", "processor.timeWindow", function () {

    var processor = this.get("processor"),
        fromEventPos = processor.timeToPositionPercent(this.get("fromEvent.time")),
        toEventPos = processor.timeToPositionPercent(this.get("toEvent.time")),
        color = this.get("bar.color") || this.get("process").getBarColor(this.get("barIndex"));

    if(fromEventPos && toEventPos) {
      Ember.run.later(this, function () {
        this.$().show();
        this.$(".event-bar").css({
          left: fromEventPos + "%",
          right: (100 - toEventPos) + "%",
          "background-color": color,
          "border-color": this.get("process").getColor()
        });
      });
    }
    else {
      this.$().hide();
    }
  }),

  sendMouseAction: function (name, mouseEvent) {
    this.sendAction(name, "event-bar", this.get("process"), {
      mouseEvent: mouseEvent,
      bar: this.get("bar"),
      fromEvent: this.get("fromEvent"),
      toEvent: this.get("toEvent")
    });
  },

  mouseEnter: function (mouseEvent) {
    this.sendMouseAction("showTooltip", mouseEvent);
  },

  mouseLeave: function (mouseEvent) {
    this.sendMouseAction("hideTooltip", mouseEvent);
  },

  mouseUp: function (mouseEvent) {
    this.sendMouseAction("click", mouseEvent);
  }

});
