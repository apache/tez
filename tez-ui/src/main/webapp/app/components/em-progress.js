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
import layout from '../templates/components/em-progress';

export default Ember.Component.extend({
  layout: layout,

  value: 0,
  valueMin: 0,
  valueMax: 1,

  classNames: ["em-progress-container"],
  classNameBindings: ["animated", "striped"],

  striped: false,
  style: null,

  progressBar: null,

  widthPercent: Ember.computed("value", "valueMin", "valueMax", function () {
    var value = this.get("value"),
        valueMin = this.get("valueMin"),
        valueMax = this.get("valueMax");

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

  progressText: Ember.computed("widthPercent", function () {
    var percent = parseInt(this.get("widthPercent"));
    if(isNaN(percent)) {
      percent = 0;
    }
    return percent + "%";
  }),

  animated: Ember.computed("widthPercent", "striped", function () {
    return this.get('striped') && this.get('widthPercent') > 0 && this.get('widthPercent') < 100;
  }),

  progressBarClasses: Ember.computed("style", "striped", "animated", function () {
    var classes = [],
        style = this.get("style");

    if(style) {
      classes.push(`progress-bar-${style}`);
    }
    if(this.get("striped")) {
      classes.push("progress-bar-striped");
    }
    if(this.get("animated")) {
      classes.push("active");
    }

    return classes.join(" ");
  }),

  renderProgress: Ember.observer("progressBar", "widthPercent", function () {
    var widthPercent = this.get('widthPercent');
    this.get("progressBar").width(widthPercent + "%");
  }),

  didInsertElement: function () {
    Ember.run.scheduleOnce('afterRender', this, function() {
      this.setProperties({
        progressBar: this.$(".progress-bar")
      });
    });
  },

  willDestroy: function () {
    this.setProperties({
      progressBar: null,
    });
  }
});
