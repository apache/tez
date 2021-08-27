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

import { computed, get } from '@ember/object';

import isIOCounter from '../utils/misc';
import ColumnDefinition from './column-definition';

/*
 * Returns a counter value from for a row
 * @param row
 * @return value
 */
function getCounterContent(row) {
  var counter = get(row, this.contentPath);

  if(counter) {
    counter = counter[this.counterGroupName];
    if(counter) {
      return counter[this.counterName] || null;
    }
    return null;
  }
}

var CounterColumnDefinition = ColumnDefinition.extend({
  counterName: "",
  counterGroupName: "",

  observePath: true,
  contentPath: "counterGroupsHash",

  getCellContent: getCounterContent,
  getSearchValue: getCounterContent,
  getSortValue: getCounterContent,

  id: computed("counterName", "counterGroupName", function () {
    var groupName = this.counterGroupName,
        counterName = this.counterName;
    return `${groupName}/${counterName}`;
  }),

  groupDisplayName: computed("counterGroupName", function () {
    var displayName = this.counterGroupName;

    // Prune dotted path
    displayName = displayName.substr(displayName.lastIndexOf('.') + 1);

    if(isIOCounter(displayName)) {
      displayName = displayName.replace("_INPUT_", " to Input-");
      displayName = displayName.replace("_OUTPUT_", " to Output-");
    }

    // Prune counter text
    displayName = displayName.replace("Counter_", " - ");
    displayName = displayName.replace("Counter", "");

    return displayName;
  }),

  headerTitle: computed("groupDisplayName", "counterName", function () {
    var groupName = this.groupDisplayName,
        counterName = this.counterName;
    return `${groupName} - ${counterName}`;
  }),
});

CounterColumnDefinition.make = function (rawDefinition) {
  if(Array.isArray(rawDefinition)) {
    return rawDefinition.map(function (def) {
      return CounterColumnDefinition.create(def);
    });
  }
  else if(typeof rawDefinition === 'object') {
    return CounterColumnDefinition.create(rawDefinition);
  }
  else {
    throw new Error("rawDefinition must be an Array or an Object.");
  }
};

export default CounterColumnDefinition;
