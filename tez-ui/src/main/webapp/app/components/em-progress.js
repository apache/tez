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
import layout from '../templates/components/em-progress';

export default Component.extend({
  layout: layout,

  value: 0,
  valueMin: 0,
  valueMax: 1,
  striped: false,
  style: null,

  classNames: ["em-progress-container"],
  classNameBindings: ["animated", "striped"],


  progressBar: null,

  widthPercent: computed("value", "valueMin", "valueMax", function () {
    var value = this.value,
        valueMin = this.valueMin,
        valueMax = this.valueMax;

    if(value < valueMin) {
      value = valueMin;
    }
    else if(value > valueMax) {
      value = valueMax;
    }

    value -= valueMin;
    valueMax -= valueMin;

    return (value / valueMax) * 100;
  }),

  progressText: computed("widthPercent", function () {
    var percent = parseInt(this.widthPercent);
    if(isNaN(percent)) {
      percent = 0;
    }
    return percent + "%";
  }),

  animated: computed("widthPercent", "striped", function () {
    return this.striped && this.widthPercent > 0 && this.widthPercent < 100;
  }),

  progressBarClasses: computed("style", "striped", "animated", function () {
    var classes = [],
        style = this.style;

    if(style) {
      classes.push(`progress-bar-${style}`);
    }
    if(this.striped) {
      classes.push("progress-bar-striped");
    }
    if(this.animated) {
      classes.push("active");
    }

    return classes.join(" ");
  }),

  renderProgress: observer("progressBar", "widthPercent", function () {
    var widthPercent = this.widthPercent;
    this.progressBar.style.width = widthPercent + "%";
  }),

  didInsertElement: function () {
    this._super(...arguments);
    scheduleOnce('afterRender', this, function() {
      this.setProperties({
        progressBar: this.element.querySelector(".progress-bar")
      });
    });
  },

  willDestroy: function () {
    this._super(...arguments);
    this.setProperties({
      progressBar: null,
    });
  }
});
