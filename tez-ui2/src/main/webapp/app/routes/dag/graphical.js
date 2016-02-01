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
import MultiAmPollsterRoute from '../multi-am-pollster';

export default MultiAmPollsterRoute.extend({
  title: "Graphical View",

  loaderNamespace: "dag",

  setupController: function (controller, model) {
    this._super(controller, model);
    Ember.run.later(this, "startCrumbBubble");
  },

  load: function (value, query, options) {
    return this.get("loader").query('vertex', {
      dagID: this.modelFor("dag").get("id")
    }, options);
  },

  _loadedValueObserver: Ember.observer("loadedValue", function () {
    var loadedValue = this.get("loadedValue"),
        records = [];

    if(loadedValue) {
      loadedValue.forEach(function (record) {
        records.push(record);
      });

      this.set("polledRecords", records);
    }
    Ember.run.later(this, "setViewHeight", 100);
  }),

  setViewHeight: function () {
    var container = Ember.$('#graphical-view-component-container'),
        offset;

    if(container) {
      offset = container.offset();
      container.height(
        Math.max(
          // 50 pixel is left at the bottom
          offset ? Ember.$(window).height() - offset.top - 70 : 0,
          500 // Minimum dag view component container height
        )
      );
    }
  },

  actions: {
    didTransition: function () {
      Ember.$(window).on('resize', this.setViewHeight);
      this._super();
      return true;
    },
    willTransition: function () {
      Ember.$(window).off('resize', this.setViewHeight);
      this._super();
      return true;
    },
  }

});
