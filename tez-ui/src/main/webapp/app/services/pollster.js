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

import { computed, observer } from '@ember/object';
import { oneWay } from '@ember/object/computed';
import { on } from '@ember/object/evented';
import { allSettled } from 'rsvp';
import { later } from '@ember/runloop';
import Service, { inject as service } from '@ember/service';
import MoreObject from '../utils/more-object';

const STATE_STORAGE_KEY = "pollingIsActive",
      DEFAULT_LABEL = "default_label";

export default Service.extend({
  localStorage: service("localStorage"),
  env: service("env"),

  interval: oneWay("env.app.pollingInterval"),

  active: false,
  isPolling: false,
  scheduleID: null,

  polls: {},
  pollCount: 0,

  initState: on("init", function () {
    var state = this.localStorage.get(STATE_STORAGE_KEY);

    if(state === undefined || state === null) {
      state = true;
    }
    later(this, function () {
      this.set("active", state);
    });
  }),
  stateObserver: observer("active", function () {
    this.localStorage.set(STATE_STORAGE_KEY, this.active);
    this.callPoll();
  }),

  isReady: computed("active", "pollCount", function () {
    return !!(this.active && this.pollCount);
  }),

  callPoll: function () {
    var that = this;
    this.unSchedulePoll();
    if(this.isReady && !this.isPolling) {
      var pollsPromises = [];

      this.set("isPolling", true);

      MoreObject.forEach(this.polls, function (label, pollDef) {
        pollsPromises.push(pollDef.callback.call(pollDef.context));
      });

      allSettled(pollsPromises).finally(function () {
        that.set("isPolling", false);
        that.schedulePoll();
      });
    }
  },

  schedulePoll: function () {
    this.set("scheduleID", setTimeout(this.callPoll.bind(this), this.interval));
  },
  unSchedulePoll: function () {
    clearTimeout(this.scheduleID);
  },

  setPoll: function (pollFunction, context, label) {
    var polls = this.polls,
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
    var polls = this.polls,
        pollCount;

    label = label || DEFAULT_LABEL;
    delete polls[label];
    this.set("pollCount", pollCount = Object.keys(polls).length);

    if(!pollCount) {
      this.unSchedulePoll();
    }
  }
});
