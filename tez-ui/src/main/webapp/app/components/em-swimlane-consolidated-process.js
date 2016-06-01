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
  processor: null,
  focusedProcess: null,

  classNames: ["em-swimlane-consolidated-process"],
  classNameBindings: ['focused'],

  focused: Ember.computed("process", "focusedProcess", function () {
    return this.get("process") === this.get("focusedProcess");
  }),

  fromPos: Ember.computed("process.consolidateStartTime", "processor.timeWindow", function () {
    var time = this.get("process.consolidateStartTime");
    if(time) {
      return this.get("processor").timeToPositionPercent(time);
    }
  }),

  toPos: Ember.computed("process.consolidateEndTime", "processor.timeWindow", function () {
    var time = this.get("process.consolidateEndTime");
    if(time) {
      return this.get("processor").timeToPositionPercent(time);
    }
  }),

  didInsertElement: Ember.observer("fromPos", "toPos", function () {
    Ember.run.scheduleOnce('afterRender', this, function() {
      var fromPos = this.get("fromPos"),
          toPos = this.get("toPos"),
          thisElement = this.$();

      if(fromPos && toPos) {
        thisElement.show();
        thisElement.css({
          left: fromPos + "%",
          right: (100 - toPos) + "%",
          "background-color": this.get("process").getConsolidateColor(),
          "z-index": parseInt(toPos - fromPos)
        });
      }
      else {
        thisElement.hide();
      }
    });
  }),

  sendMouseAction: function (name, mouseEvent) {
    var fromPos = this.get("fromPos") || 0,
        toPos = this.get("toPos") || 0;

    this.sendAction(name, "consolidated-process", this.get("process"), {
      mouseEvent: mouseEvent,
      contribution: parseInt(toPos - fromPos)
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
