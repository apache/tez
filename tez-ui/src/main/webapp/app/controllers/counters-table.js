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

import { A } from '@ember/array';
import EmberObject, { observer } from '@ember/object';
import MoreObject from '../utils/more-object';

import TableController from './table';
import ColumnDefinition from '../utils/column-definition';

export default TableController.extend({
  counters: A(),
  countersCount: 0, // Because Ember Array doesn't handle length well

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

  _countersObserver: observer("model.counterGroupsHash", function () {
    var counterGroupsHash = this.get("model.counterGroupsHash"),
        counters = this.counters,
        counterIndex = 0;

    if(counterGroupsHash) {
      MoreObject.forEach(counterGroupsHash, function (groupName, countersHash) {
        if(countersHash) {
          MoreObject.forEach(countersHash, function (counterName, counterValue) {
            let counterRow = counters.get(counterIndex);
            if(!counterRow) {
              counterRow = EmberObject.create();
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
