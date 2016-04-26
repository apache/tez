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

  classNames: ['error-bar'],
  classNameBindings: ['visible', 'detailsAvailable'],

  code: null,
  message: null,
  details: null,
  stack: null,

  showDetails: false,

  displayTimerId: 0,

  _errorObserver: Ember.observer("error", function () {
    var error = this.get("error"),

        code = Ember.get(error, "errors.0.status"),
        title = Ember.get(error, "errors.0.title"),
        message = error.message || "Error",
        details = Ember.get(error, "errors.0.detail") || "",
        stack = error.stack,
        lineEndIndex = Math.min(message.indexOf('\n'), message.indexOf('<br'));

    if(code === "0") {
      code = "";
    }

    if(title) {
      message += ". " + title;
    }

    if(lineEndIndex > 0) {
      if(details) {
        details = "\n" + details;
      }
      details = message.substr(lineEndIndex) + details;
      message = message.substr(0, lineEndIndex);
    }

    if(details) {
      details += "\n";
    }

    if(error) {
      this.setProperties({
        code: code,
        message: message,
        details: details,
        stack: stack,

        detailsAvailable: !!(details || stack),
        visible: true
      });

      this.clearTimer();
      this.set("displayTimerId", setTimeout(this.close.bind(this), DISPLAY_TIME));
    }
    else {
      this.close();
    }
  }),

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
