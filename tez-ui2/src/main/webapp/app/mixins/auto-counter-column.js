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

var MoreObject = more.Object;

export default Ember.Mixin.create({
  columnSelectorMessage: "<span class='per-io'>Per-IO counter</span> selection wouldn't persist.",

  getCounterColumns: function () {
    var columns = [],
        records = this.get("model"),
        counterHash = {};

    this._super().forEach(function (column) {
      var groupHash =
          counterHash[column.counterGroupName] =
          counterHash[column.counterGroupName] || {};
      groupHash[column.counterName] = column.counterName;
    });

    if(records) {
      records.forEach(function (record) {
        let counterGroupsHash = Ember.get(record, 'counterGroupsHash');

        if(counterGroupsHash) {
          MoreObject.forEach(counterGroupsHash, function (groupName, countersHash) {
            var groupHash =
                counterHash[groupName] =
                counterHash[groupName] || {};

            MoreObject.forEach(countersHash, function (counterName) {
              groupHash[counterName] = counterName;
            });
          });
        }
      });
    }

    MoreObject.forEach(counterHash, function (groupName, counters) {
      MoreObject.forEach(counters, function (counterName) {
        columns.push({
          counterName: counterName,
          counterGroupName: groupName
        });
      });
    });

    return columns;
  }
});
