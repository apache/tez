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
  blocking: null,

  processor: null,

  classNames: ["em-swimlane-blocking-event"],

  blockingEvent: computed("process.blockingEventName",
      "process.events.@each.name", function () {
    var events = this.get("process.events"),
        blockingEventName = this.get("process.blockingEventName");

    return events.find(function (event) {
      return event.name === blockingEventName;
    });
  }),

  didInsertElement: observer("blockingEvent.time", "processor.timeWindow", function () {
    var blockTime = this.get("blockingEvent.time"),
        blockerEventHeight;

    if(blockTime && this.get("blocking.endEvent.time") >= blockTime) {
      blockerEventHeight = (this.get("blocking.index") - this.get("process.index")) * 30;

    this.set('_handleMouseEnter', this.handleMouseEnter.bind(this));
    this.element.addEventListener('mouseenter', this._handleMouseEnter);
    this.set('_handleMouseLeave', this.handleMouseLeave.bind(this));
    this.element.addEventListener('mouseleave', this._handleMouseLeave);
    this.set('_handleMouseUp', this.handleMouseUp.bind(this));
    this.element.addEventListener('mouseup', this._handleMouseUp);

      scheduleOnce('afterRender', this, function() {
        this.element.style.left = this.processor.timeToPositionPercent(blockTime) + "%";
        let eventLine = this.element.querySelector('.event-line');
        eventLine.style.height = `${blockerEventHeight}px`;
        eventLine.style.borderColor = this.process.getColor();
      });
    }
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

    this.showTooltip("blocking-event", this.process, {
      mouseEvent: mouseEvent,
      blocking: this.blocking,
      blockingEvent: this.blockingEvent
    });
  },

  handleMouseLeave: function () {
    this.hideTooltip();
  },

  handleMouseUp: function () {
    this.routeToVertex(this.process.vertex.entityID);
  }
});
