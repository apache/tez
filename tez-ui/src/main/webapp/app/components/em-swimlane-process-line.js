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
import { observer } from '@ember/object';
import { scheduleOnce } from '@ember/runloop';

export default Component.extend({

  process: null,
  processor: null,

  didInsertElement: observer("process.startEvent.time",
      "process.endEvent.time", "processor.timeWindow", function () {
    var processor = this.processor,
        startPos = processor.timeToPositionPercent(this.get("process.startEvent.time")),
        endPos = processor.timeToPositionPercent(this.get("process.endEvent.time"));

    this.set('_handleMouseEnter', this.handleMouseEnter.bind(this));
    this.element.addEventListener('mouseenter', this._handleMouseEnter);
    this.set('_handleMouseLeave', this.handleMouseLeave.bind(this));
    this.element.addEventListener('mouseleave', this._handleMouseLeave);
    this.set('_handleMouseUp', this.handleMouseUp.bind(this));
    this.element.addEventListener('mouseup', this._handleMouseUp);

    scheduleOnce('afterRender', this, function() {
      let processLine = this.element.querySelector('.process-line');
      processLine.style.left = startPos + "%";
      processLine.style.right = (100 - endPos) + "%";
      processLine.style.backgroundColor = this.process.getColor();
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

    this.showTooltip("process-line", this.process, {
      mouseEvent: mouseEvent,
    });
  },

  handleMouseLeave: function () {
    this.hideTooltip();
  },

  handleMouseUp: function () {
    this.routeToVertex(this.process.vertex.entityID);
  }
});
