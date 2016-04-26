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

import TableController from './table';
import ColumnDefinition from 'em-table/utils/column-definition';

var MoreObject = more.Object;

export default TableController.extend({
  counters: Ember.A(),
  countersCount: 0, // Because Ember.Array doesn't handle length well

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
    observePath: true
  }]),

  _countersObserver: Ember.observer("model.counterGroupsHash", function () {
    var counterGroupsHash = this.get("model.counterGroupsHash"),
        counters = this.get("counters"),
        counterIndex = 0;

    if(counterGroupsHash) {
      MoreObject.forEach(counterGroupsHash, function (groupName, countersHash) {
        if(countersHash) {
          MoreObject.forEach(countersHash, function (counterName, counterValue) {
            let counterRow = counters.get(counterIndex);
            if(!counterRow) {
              counterRow = Ember.Object.create();
              counters.push(counterRow);
            }

            counterRow.setProperties({
              groupName: groupName,
              counterName: counterName,
              counterValue: counterValue
            });
            counterIndex++;
          });
        }
      });
    }

    this.set("countersCount", counterIndex);
  })
});
