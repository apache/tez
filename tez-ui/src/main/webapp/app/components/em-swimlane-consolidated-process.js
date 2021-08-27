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

import Component from '@ember/component';
import { computed, observer } from '@ember/object';
import { scheduleOnce } from '@ember/runloop';

export default Component.extend({

  process: null,
  processor: null,
  focusedProcess: null,

  classNames: ["em-swimlane-consolidated-process"],
  classNameBindings: ['focused'],

  focused: computed("process", "focusedProcess", function () {
    return this.process === this.focusedProcess;
  }),

  fromPos: computed("process.consolidateStartTime", "processor.timeWindow", function () {
    var time = this.get("process.consolidateStartTime");
    if(time) {
      return this.processor.timeToPositionPercent(time);
    }
  }),

  toPos: computed("process.consolidateEndTime", "processor.timeWindow", function () {
    var time = this.get("process.consolidateEndTime");
    if(time) {
      return this.processor.timeToPositionPercent(time);
    }
  }),

  didInsertElement: observer("fromPos", "toPos", function () {

    this.set('_handleMouseEnter', this.handleMouseEnter.bind(this));
    this.element.addEventListener('mouseenter', this._handleMouseEnter);
    this.set('_handleMouseLeave', this.handleMouseLeave.bind(this));
    this.element.addEventListener('mouseleave', this._handleMouseLeave);
    this.set('_handleMouseUp', this.handleMouseUp.bind(this));
    this.element.addEventListener('mouseup', this._handleMouseUp);

    scheduleOnce('afterRender', this, function() {
      var fromPos = this.fromPos,
          toPos = this.toPos;

      if(fromPos && toPos) {
        this.element.style.display = 'block';
        this.element.style.left = fromPos + "%";
        this.element.style.right = (100 - toPos) + "%";
        this.element.style.backgroundColor = this.process.getConsolidateColor();
        this.element.style.zIndex = parseInt(toPos - fromPos);
      }
      else {
        this.element.style.display = 'none';
      }
    });
  }),

  willDestroyElement: function () {
    if (this._handleMouseEnter) {
      this.element.removeEventListener('mouseenter', this._handleMouseEnter);
    }
    if (this._handleMouseLeave) {
      this.element.removeEventListener('mouseleave', this._handleMouseLeave);
    }
    if (this._handleMouseUp) {
      this.element.removeEventListener('mouseup', this._handleMouseUp);
    }
  },

  handleMouseEnter: function (mouseEvent) {

    var fromPos = this.fromPos || 0,
        toPos = this.toPos || 0;

    this.showSwimlaneTooltip("consolidated-process", this.process, {
      mouseEvent: mouseEvent,
      contribution: parseInt(toPos - fromPos)
    });
  },

  handleMouseLeave: function () {
    this.hideSwimlaneTooltip();
  },

  handleMouseUp: function () {
    this.routeToVertex(this.process.vertex.entityID);
  }
});
