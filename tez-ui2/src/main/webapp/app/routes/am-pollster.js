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

import PollsterRoute from './pollster';

var MoreObject = more.Object;

export default PollsterRoute.extend({

  countersToPoll: null,

  onRecordPoll: function (record) {
    var query = {},
        countersToPoll = this.get("countersToPoll");

    if(countersToPoll !== null) {
      query.counters = countersToPoll;
    }

    return this.get("loader").loadNeed(record, "am", {reload: true}, query);
  },

  onPollFailure: function (error) {
    var that = this,
        record = this.get("polledRecords.0");

    this.get("loader").queryRecord("appRm", record.get("appID"), {reload: true}).then(function (appRm) {
      if(appRm.get('isComplete')) {
        that.scheduleReload();
      }
      else {
        that.send("error", error);
      }
    }, function (error) {
      that.send("error", error);
      that.scheduleReload();
    });
  },

  scheduleReload: function () {
    this.set("polledRecords", null);
    Ember.run.debounce(this, "reload", this.get("polling.interval") * 2);
  },

  reload: function () {
    this.set("polledRecords", null);
    this.send("reload");
  },

  actions: {
    countersToPollChanged: function (counterColumnDefinitions) {
      var counterGroupHash = {},
          counterGroups = [];

      if(counterColumnDefinitions){
        counterColumnDefinitions.forEach(function (definition) {
          var counterGroupName = definition.get("counterGroupName"),
              counterNames = counterGroupHash[counterGroupName];
          if(!counterNames) {
            counterNames = counterGroupHash[counterGroupName] = [];
          }
          counterNames.push(definition.get("counterName"));
        });

        MoreObject.forEach(counterGroupHash, function (groupName, counters) {
          counters = counters.join(",");
          counterGroups.push(`${groupName}/${counters}`);
        });
      }

      this.set("countersToPoll", counterGroups.join(";"));
    }
  }

});
