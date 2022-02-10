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

import { computed } from '@ember/object';
import { attr } from '@ember-data/model';

import TimedModel from './timed';

export default TimedModel.extend({

  needs:{
    app: {
      type: ["appRm", "AhsApp"],
      idKey: "appID",
      silent: true
    }
  },

  appID: computed("entityID", function () {
    var idParts = this.entityID.split("_");
    return `application_${idParts[1]}_${idParts[2]}`;
  }),
  app: attr("object"), // Either RMApp or AHSApp

  atsStatus: attr("string"),
  status: computed("atsStatus", "app.status", "app.finalStatus", function () {
    var status = this.atsStatus,
        yarnStatus = this.get("app.status");

    if (status !== 'RUNNING' || (yarnStatus !== 'FINISHED' && yarnStatus !== 'KILLED' && yarnStatus !== 'FAILED')) {
      return status;
    }

    if (yarnStatus === 'KILLED' || yarnStatus === 'FAILED') {
      return yarnStatus;
    }

    return this.get("app.finalStatus");
  }),

  progress: computed("status", function () {
    return this.status === "SUCCEEDED" ? 1 : null;
  }),

  // Hash will be created only on demand, till then counters will be stored in _counterGroups
  _counterGroups: attr('object'),
  counterGroupsHash: computed("_counterGroups", function () {
    var counterHash = {},
        counterGroups = this._counterGroups || [];

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

  diagnostics: attr('string'),

  events: attr('object'),

});
