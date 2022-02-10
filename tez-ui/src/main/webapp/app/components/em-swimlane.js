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

import { A } from '@ember/array';
import Component from '@ember/component';
import { action, computed, observer } from '@ember/object';
import { on } from '@ember/object/evented';
import { scheduleOnce } from '@ember/runloop';

import Processor from '../utils/processor';
import Process from '../utils/process';

export default Component.extend({

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

  startTime: computed('processes.0.startEvent.time', 'processes.@each.startEvent', function () {
    var startTime = this.get("processes.0.startEvent.time");
    this.processes.forEach(function (process) {
      var time = process.get("startEvent.time");
      if(startTime > time){
        startTime = time;
      }
    });
    return startTime;
  }),
  endTime: computed('processes.0.endEvent.time', 'processes.@each.endEvent', function () {
    var endTime = this.get("processes.0.endEvent.time");
    this.processes.forEach(function (process) {
      var time = process.get("endEvent.time");
      if(endTime < time){
        endTime = time;
      }
    });
    return endTime;
  }),

  processorSetup: on("init", observer("startTime", "endTime", "processes.length", function () {
    this.processor.setProperties({
      startTime: this.startTime,
      endTime: this.endTime,
      processCount: this.get("processes.length")
    });
  })),

  didInsertElement: function () {
    this._super(...arguments);
    scheduleOnce('afterRender', this, function() {
      this.onZoom();
      this.listenScroll();
    });
  },

  onZoom: observer("zoom", function () {
    var zoom = this.zoom;
    this.element.querySelector('.zoom-panel').style.width = `${zoom}%`;
  }),

  handleScroll: function (scrollEvent) {
    this.set('scroll', scrollEvent.target.scrollLeft);
  },

  listenScroll: function () {
    this.set('_handleScroll', this.handleScroll.bind(this));
    this.element.querySelector('.process-visuals').addEventListener('scroll', this._handleScroll);
  },

  willDestroyElement: function () {
    this._super(...arguments);
    if(this._handleScroll) {
      this.element.querySelector('.process-visuals').removeEventListener('scroll', this._handleScroll);
      this._handleScroll = null;
    }
  },

  normalizedProcesses: computed('processes.@each.blockers', 'processor', function () {
    var processes = this.processes,
        normalizedProcesses,
        idHash = {},
        containsBlockers = false,
        processor = this.processor;

    // Validate and reset blocking
    processes.forEach(function (process) {
      if(!(process instanceof Process)) {
        console.error("em-swimlane : Unknown type, must be of type Process");
      }

      if(process.get("blockers.length")) {
        containsBlockers = true;
      }
      process.set("blocking", A());
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

    return A(normalizedProcesses);
  }),

  showSwimlaneTooltip: action(function (type, process, options) {
    this.set("tooltipContents", process.getTooltipContents(type, options));
    this.set("focusedProcess", process);
  }),

  hideSwimlaneTooltip: action(function () {
    this.set("tooltipContents", null);
    this.set("focusedProcess", null);
  }),
});
