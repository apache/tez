/*global more*/
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

const STATE_STORAGE_KEY = "pollingIsActive",
      DEFAULT_LABEL = "default_label";

var MoreObject = more.Object;

export default Ember.Service.extend({
  localStorage: Ember.inject.service("localStorage"),
  env: Ember.inject.service("env"),

  interval: Ember.computed.oneWay("env.app.pollingInterval"),

  active: false,
  isPolling: false,
  scheduleID: null,

  polls: {},
  pollCount: 0,

  initState: Ember.on("init", function () {
    var state = this.get("localStorage").get(STATE_STORAGE_KEY);

    if(state === undefined || state === null) {
      state = true;
    }
    Ember.run.later(this, function () {
      this.set("active", state);
    });
  }),
  stateObserver: Ember.observer("active", function () {
    this.get("localStorage").set(STATE_STORAGE_KEY, this.get("active"));
    this.callPoll();
  }),

  isReady: Ember.computed("active", "pollCount", function () {
    return !!(this.get("active") && this.get("pollCount"));
  }),

  callPoll: function () {
    var that = this;
    this.unSchedulePoll();
    if(this.get("isReady") && !this.get("isPolling")) {
      var pollsPromises = [];

      this.set("isPolling", true);

      MoreObject.forEach(this.get("polls"), function (label, pollDef) {
        pollsPromises.push(pollDef.callback.call(pollDef.context));
      });

      Ember.RSVP.allSettled(pollsPromises).finally(function () {
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

  setPoll: function (pollFunction, context, label) {
    var polls = this.get("polls"),
        pollCount;

    label = label || DEFAULT_LABEL;
    polls[label] = {
      context: context,
      callback: pollFunction,
    };
    this.set("pollCount", pollCount = Object.keys(polls).length);

    this.callPoll();
  },
  resetPoll: function (label) {
    var polls = this.get("polls"),
        pollCount;

    label = label || DEFAULT_LABEL;
    delete polls[label];
    this.set("pollCount", pollCount = Object.keys(polls).length);

    if(!pollCount) {
      this.unSchedulePoll();
    }
  }
});
