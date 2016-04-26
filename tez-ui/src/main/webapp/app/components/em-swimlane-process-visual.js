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

const BUBBLE_DIA = 10; // Same as that in css

export default Ember.Component.extend({

  process: null,
  processor: null,
  focusedProcess: null,

  classNames: ["em-swimlane-process-visual"],

  actions: {
    showTooltip: function(type, process, options) {

      if(type === "event") {
        let clientX = options.mouseEvent.clientX,
            events = process.get("events"),
            eventsUnderMouse = [];

        this.$(".em-swimlane-event").each(function (index) {
          var offsetLeft = Ember.$(this).offset().left;

          if(clientX >= offsetLeft - BUBBLE_DIA && clientX <= offsetLeft + BUBBLE_DIA) {
            eventsUnderMouse.push(events[index]);
          }
        });

        if(events.length) {
          eventsUnderMouse.sort(function (eventA, eventB) {
            return eventA.time - eventB.time;
          });
          options.events = eventsUnderMouse;
        }
      }

      this.sendAction("showTooltip", type, process, options);
    },

    hideTooltip: function(type, process, options) {
      this.sendAction("hideTooltip", type, process, options);
    },
    click: function (type, process, options) {
      this.sendAction("click", type, process, options);
    }
  }

});
