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

  bar: null,
  barIndex: 0,

  process: null,
  processor: null,

  classNames: ["em-swimlane-event-bar"],

  fromEvent: computed("process.events.@each.name", "bar.fromEvent", function () {
    var events = this.get("process.events"),
        fromEventName = this.get("bar.fromEvent");
    return events.find(function (event) {
      return event.name === fromEventName;
    });
  }),
  toEvent: computed("process.events.@each.name", "bar.toEvent", function () {
    var events = this.get("process.events"),
        toEventName = this.get("bar.toEvent");
    return events.find(function (event) {
      return event.name === toEventName;
    });
  }),

  didInsertElement: observer("fromEvent.time", "toEvent.time", "barIndex", "processor.timeWindow", function () {

    var processor = this.processor,
      fromEventPos = processor.timeToPositionPercent(this.get("fromEvent.time")),
      toEventPos = processor.timeToPositionPercent(this.get("toEvent.time")),
      color = this.get("bar.color") || this.process.getBarColor(this.barIndex);

    this.set('_handleMouseEnter', this.handleMouseEnter.bind(this));
    this.element.addEventListener('mouseenter', this._handleMouseEnter);
    this.set('_handleMouseLeave', this.handleMouseLeave.bind(this));
    this.element.addEventListener('mouseleave', this._handleMouseLeave);
    this.set('_handleMouseUp', this.handleMouseUp.bind(this));
    this.element.addEventListener('mouseup', this._handleMouseUp);

    scheduleOnce('afterRender', this, function() {
      if(fromEventPos && toEventPos) {
        this.element.style.display = 'block';
        let eventBar = this.element.querySelector('.event-bar');
        eventBar.style.left = fromEventPos + "%";
        eventBar.style.right = (100 - toEventPos) + "%";
        eventBar.style.backgroundColor = color;
        eventBar.style.borderColor = this.process.getColor();
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

    this.showTooltip("event-bar", this.process, {
      mouseEvent: mouseEvent,
      bar: this.bar,
      fromEvent: this.fromEvent,
      toEvent: this.toEvent
    });
  },

  handleMouseLeave: function () {
    this.hideTooltip();
  },

  handleMouseUp: function () {
    this.routeToVertex(this.process.vertex.entityID);
  }
});
