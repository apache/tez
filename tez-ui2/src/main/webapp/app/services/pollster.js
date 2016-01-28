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

const STATE_STORAGE_KEY = "pollingIsActive";

export default Ember.Service.extend({
  localStorage: Ember.inject.service("localStorage"),
  env: Ember.inject.service("env"),

  interval: Ember.computed.oneWay("env.app.pollingInterval"),

  active: false,
  isPolling: false,
  scheduleID: null,

  poll: null,
  pollContext: null,

  initState: Ember.on("init", function () {
    Ember.run.later(this, function () {
      this.set("active", this.get("localStorage").get(STATE_STORAGE_KEY));
    });
  }),
  stateObserver: Ember.observer("active", function () {
    this.get("localStorage").set(STATE_STORAGE_KEY, this.get("active"));
    this.callPoll();
  }),

  isReady: Ember.computed("active", "poll", function () {
    return this.get("active") && this.get("poll");
  }),

  callPoll: function () {
    var that = this;
    this.unSchedulePoll();
    if(this.get("isReady") && !this.get("isPolling")) {
      this.set("isPolling", true);
      this.get("poll").call(this.get("pollContext")).finally(function () {
        that.set("isPolling", false);
        that.schedulePoll();
      });
    }
  },

  schedulePoll: function () {
    this.set("scheduleID", setTimeout(this.callPoll.bind(this), this.get("interval")));
  },
  unSchedulePoll: function () {
    clearTimeout(this.get("scheduleID"));
  },

  setPoll: function (pollFunction, context) {
    this.setProperties({
      pollContext: context,
      poll: pollFunction,
    });
    this.callPoll();
  },
  resetPoll: function () {
    this.unSchedulePoll();
    this.setProperties({
      poll: null,
      pollContext: null
    });
  }
});
