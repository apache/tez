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
  event: null,

  processor: null,

  classNames: ["em-swimlane-event"],

  didInsertElement: Ember.observer("event.time", "processor.timeWindow", function () {
    var color = this.get("process").getColor();

    this.$(".event-line").css("border-color", color);
    this.$(".event-bubble").css("border-color", color);

    Ember.run.later(this, function () {
      this.$().css({
        "left": this.get("processor").timeToPositionPercent(this.get("event.time")) + "%"
      });
    });
  }),

  sendMouseAction: function (name, mouseEvent) {
    this.sendAction(name, "event", this.get("process"), {
      mouseEvent: mouseEvent,
      events: [this.get("event")]
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
