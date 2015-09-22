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
App.Helpers.Pollster = Ember.Object.extend({
  interval: function() {
    return this.get('_interval') || 10000; // Time between polls (in ms)
  }.property().readOnly(),

  // Schedules the function `f` to be executed every `interval` time.
  // if runImmediate is set first run is scheduled immedietly
  schedule: function(f, runImmediete) {
    var timer = this.get('timer');
    if(timer) {
      return timer;
    }
    return Ember.run.later(this, function() {
      f.apply(this);
      this.set('timer', null);
      this.set('timer', this.schedule(f));
    }, this.get('interval'));
  },

  // Stops the pollster
  stop: function() {
    Ember.run.cancel(this.get('timer'));
    this.set('timer', null);
  },

  // Starts the pollster, i.e. executes the `onPoll` function every interval.
  start: function(runImmediate, interval) {
    if (!!interval && interval > 1000) {
      this.set('_interval', interval)
    }
    var callback = this.get('onPoll');
    if (runImmediate) {
      callback.apply(this);
    }
    this.set('timer', this.schedule(callback, runImmediate));
  },

  onPoll: function(){
    // Issue JSON request and add data to the store
  }
});