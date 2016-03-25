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
  events: [],

  bars: [],
  bar: null,
  barIndex: 0,

  classNames: ["em-swimlane-event-bar"],

  fromEvent: Ember.computed("events.length", "bar.fromEvent", function () {
    var events = this.get("events"),
        fromEventName = this.get("bar.fromEvent");
    return events.find(function (event) {
      return event.name === fromEventName;
    });
  }),
  toEvent: Ember.computed("events.length", "bar.toEvent", function () {
    var events = this.get("events"),
        toEventName = this.get("bar.toEvent");
    return events.find(function (event) {
      return event.name === toEventName;
    });
  }),

  didInsertElement: Ember.observer("fromEvent.pos", "toEvent.pos", "barIndex", function () {
    var fromEventPos = this.get("fromEvent.pos"),
        toEventPos = this.get("toEvent.pos"),
        color = this.get("bar.color") ||
            this.get("process").getColor(1 - (this.get("barIndex") / this.get("bars.length")));

    if(fromEventPos && toEventPos) {
      this.$().show();
      this.$(".event-bar").css({
        left: fromEventPos + "%",
        right: (100 - toEventPos) + "%",
        "background-color": color,
        "border-color": this.get("process").getColor()
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
