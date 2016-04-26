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
import AbstractRoute from './abstract';

export default AbstractRoute.extend({
  polling: Ember.inject.service("pollster"),

  // Todo - Change name to recordsToPoll
  polledRecords: null,

  // Must be implemented by inheriting classes
  onRecordPoll: function (val) {return val;},
  onPollSuccess: function (val) {return val;},
  onPollFailure: function (err) {throw(err);},

  pollData: function () {
    var polledRecords = this.get("polledRecords");

    if(!this.get("isLoading") && polledRecords) {
      polledRecords = polledRecords.map(this.onRecordPoll.bind(this));
      return Ember.RSVP.all(polledRecords).
      then(this.updateLoadTime.bind(this)).
      then(this.onPollSuccess.bind(this), this.onPollFailure.bind(this));
    }
    return Ember.RSVP.reject();
  },

  canPoll: Ember.computed("polledRecords", "loadedValue", function () {
    return this.get("polledRecords") && this.get("loadedValue");
  }),

  updateLoadTime: function (value) {
    this.send("setLoadTime", this.getLoadTime(value));
    return value;
  },

  _canPollInit: Ember.on("init", function () {
    // This sets a flag that ensures that the _canPollObserver is called whenever
    // canPoll changes. By default observers on un-used computed properties
    // are not called.
    this.get("canPoll");
  }),

  _canPollObserver: Ember.observer("canPoll", function () {
    if(this.get("canPoll")) {
      this.get("polling").setPoll(this.pollData, this);
    }
    else {
      this.get("polling").resetPoll();
    }
  }),

});
