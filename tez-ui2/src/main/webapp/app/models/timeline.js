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

import DS from 'ember-data';
import Ember from 'ember';

import AbstractModel from './abstract';

export default AbstractModel.extend({

  needs:{
    app: {
      type: ["appRm", "AhsApp"],
      idKey: "appID",
      silent: true
    }
  },

  appID: Ember.computed("entityID", function () {
    var idParts = this.get("entityID").split("_");
    return `application_${idParts[1]}_${idParts[2]}`;
  }),
  app: DS.attr("object"), // Either RMApp or AHSApp

  atsStatus: DS.attr("string"),
  status: Ember.computed("atsStatus", "app.status", "app.finalStatus", function () {
    var status = this.get("atsStatus"),
        yarnStatus = this.get("app.status");

    if (status !== 'RUNNING' || (yarnStatus !== 'FINISHED' && yarnStatus !== 'KILLED' && yarnStatus !== 'FAILED')) {
      return status;
    }

    if (yarnStatus === 'KILLED' || yarnStatus === 'FAILED') {
      return yarnStatus;
    }

    return this.get("app.finalStatus");
  }),

  progress: Ember.computed("status", function () {
    return this.get("status") === "SUCCEEDED" ? 1 : null;
  }),

  startTime: DS.attr("number"),
  endTime: DS.attr("number"),
  duration: Ember.computed("startTime", "endTime", function () {
    var duration = this.get("endTime") - this.get("startTime");
    return duration > 0 ? duration : null;
  }),

  // Hash will be created only on demand, till then counters will be stored in _counterGroups
  _counterGroups: DS.attr('object'),
  counterGroupsHash: Ember.computed("_counterGroups", function () {
    var counterHash = {},
        counterGroups = this.get("_counterGroups") || [];

    counterGroups.forEach(function (group) {
      var counters = group.counters,
          groupHash;

      groupHash = counterHash[group.counterGroupName] = counterHash[group.counterGroupName] || {};

      counters.forEach(function (counter) {
        groupHash[counter.counterName] = counter.counterValue;
      });
    });

    return counterHash;
  }),

  diagnostics: DS.attr('string'),

  events: DS.attr('object'),

});
