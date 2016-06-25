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

const DISPLAY_TIME = 30 * 1000;

export default Ember.Component.extend({

  error: null,

  visible: false,
  detailsAvailable: false,
  showDetails: false,
  displayTimerId: 0,

  classNames: ['error-bar'],
  classNameBindings: ['visible', 'detailsAvailable'],

  message: null,

  _errorObserver: Ember.on("init", Ember.observer("error", function () {
    var error = this.get("error");

    if(error) {
      this.setProperties({
        message: error.message || "Error",
        detailsAvailable: !!(error.details || error.requestInfo || error.stack),
        visible: true
      });

      this.clearTimer();
      this.set("displayTimerId", setTimeout(this.close.bind(this), DISPLAY_TIME));
    }
    else {
      this.close();
    }
  })),

  clearTimer: function () {
    clearTimeout(this.get("displayTimerId"));
  },
  close: function () {
    this.set("visible", false);
    this.clearTimer();
  },

  actions: {
    toggleDetailsDisplay: function () {
      this.toggleProperty("showDetails");
      this.clearTimer();
    },
    close: function () {
      this.close();
    }
  }
});
