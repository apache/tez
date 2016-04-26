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

import Processor from '../utils/processor';
import Process from '../utils/process';

export default Ember.Component.extend({

  classNames: ["em-swimlane"],

  processes: [],
  processor: Processor.create(),

  nameComponent: "em-swimlane-process-name",
  visualComponent: "em-swimlane-process-visual",

  tooltipContents: null,
  focusedProcess: null,
  scroll: 0,

  consolidate: false,

  zoom: 100,

  startTime: Ember.computed("processes.@each.startEvent", function () {
    var startTime = this.get("processes.0.startEvent.time");
    this.get("processes").forEach(function (process) {
      var time = process.get("startEvent.time");
      if(startTime > time){
        startTime = time;
      }
    });
    return startTime;
  }),
  endTime: Ember.computed("processes.@each.endEvent", function () {
    var endTime = this.get("processes.0.endEvent.time");
    this.get("processes").forEach(function (process) {
      var time = process.get("endEvent.time");
      if(endTime < time){
        endTime = time;
      }
    });
    return endTime;
  }),

  processorSetup: Ember.on("init", Ember.observer("startTime", "endTime", "processes.length", function () {
    this.get("processor").setProperties({
      startTime: this.get("startTime"),
      endTime: this.get("endTime"),
      processCount: this.get("processes.length")
    });
  })),

  didInsertElement: function () {
    this.onZoom();
    this.listenScroll();
  },

  onZoom: Ember.observer("zoom", function () {
    var zoom = this.get("zoom");
    this.$(".zoom-panel").css("width", `${zoom}%`);
  }),

  listenScroll: function () {
    var that = this;
    this.$(".process-visuals").scroll(function () {
      that.set("scroll", Ember.$(this).scrollLeft());
    });
  },

  willDestroy: function () {
    // Release listeners
  },

  normalizedProcesses: Ember.computed("processes.@each.blockers", function () {
    var processes = this.get("processes"),
        normalizedProcesses,
        idHash = {},
        containsBlockers = false,
        processor = this.get("processor");

    // Validate and reset blocking
    processes.forEach(function (process) {
      if(!(process instanceof Process)) {
        Ember.Logger.error("em-swimlane : Unknown type, must be of type Process");
      }

      if(process.get("blockers.length")) {
        containsBlockers = true;
      }
      process.set("blocking", Ember.A());
    });

    if(containsBlockers) {
      normalizedProcesses = [];

      // Recreate blocking list
      processes.forEach(function (process) {
        var blockers = process.get("blockers");
        if(blockers) {
          blockers.forEach(function (blocker) {
            blocker.get("blocking").push(process);
          });
        }
      });

      // Give an array of the processes in blocking order
      processes.forEach(function (process) {
        if(process.get("blocking.length") === 0) { // The root processes
          normalizedProcesses.push(process);
          normalizedProcesses.push.apply(normalizedProcesses, process.getAllBlockers());
        }
      });
      normalizedProcesses.reverse();
      normalizedProcesses = normalizedProcesses.filter(function (process, index) {
        // Filters out the recurring processes in the list (after graph traversal), we just
        // need the top processes
        var id = process.get("_id");
        if(idHash[id] === undefined) {
          idHash[id] = index;
        }
        return idHash[id] === index;
      });
    }
    else {
      normalizedProcesses = processes;
    }

    // Set process colors & index
    normalizedProcesses.forEach(function (process, index) {
      process.setProperties({
        color: processor.createProcessColor(index),
        index: index
      });
    });

    return Ember.A(normalizedProcesses);
  }),

  actions: {
    showTooltip: function (type, process, options) {
      this.set("tooltipContents", process.getTooltipContents(type, options));
      this.set("focusedProcess", process);
    },
    hideTooltip: function () {
      this.set("tooltipContents", null);
      this.set("focusedProcess", null);
    },
    click: function (type, process, options) {
      this.sendAction("click", type, process, options);
    }
  }

});
