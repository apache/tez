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

import TablePageController from './table-page';
import ColumnDefinition from 'em-table/utils/column-definition';

export default TablePageController.extend({
  columns: ColumnDefinition.make([{
    id: 'groupName',
    headerTitle: 'Group Name',
    contentPath: 'groupName',
  }, {
    id: 'counterName',
    headerTitle: 'Counter Name',
    contentPath: 'counterName',
  }, {
    id: 'counterValue',
    headerTitle: 'Counter Value',
    contentPath: 'counterValue',
  }]),

  counters: Ember.computed("model.counterGroups", function () {
    var counterGroups = this.get("model.counterGroups"),
        counterRows = [];

    if(counterGroups) {
      counterGroups.forEach(function (group) {
        var counterGroupName = group.counterGroupName,
            counters = group.counters;

        if(counters) {
          counterGroupName = counterGroupName.substr(counterGroupName.lastIndexOf('.') + 1);
          counters.forEach(function (counter) {
            counterRows.push(Ember.Object.create({
              groupName: counterGroupName,
              counterName: counter.counterName,
              counterValue: counter.counterValue
            }));
          });
        }
      });
    }

    return Ember.A(counterRows);
  })
});
