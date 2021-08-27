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

import Component from '@ember/component';
import EmberObject, { action, computed, observer, set } from '@ember/object';

import isIOCounter from '../utils/misc';

export default Component.extend({

  classNames: ['column-selector'],

  searchText: "",
  selectAll: false,

  content: null,

  options: computed("content.columns", "content.visibleColumnIDs", function () {
    var group,
        highlight = false,
        visibleColumnIDs = this.get('content.visibleColumnIDs') || {};

    return this.get('content.columns').map(function (definition) {
      var css = '';

      highlight = highlight ^ (definition.counterGroupName !== group);
      group = definition.counterGroupName;

      if(highlight) {
        css += ' highlight';
      }
      if(group && isIOCounter(group)) {
        css += ' per-io';
      }

      return EmberObject.create({
        id: definition.id,
        displayText: definition.headerTitle,
        css: css,
        selected: visibleColumnIDs[definition.id]
      });
    });
  }),

  filteredOptions: computed("options", "searchText", function () {
    var options = this.options,
        searchText = this.searchText;

    if (!searchText) {
      return options;
    }

    return options.filter(function (option) {
      return option.get('displayText').match(new RegExp(searchText, 'i'));
    });
  }),

  selectedColumnIDs: computed("options", function () {
    var columnIds = {};
    this.options.forEach(function (option) {
      columnIds[option.get("id")] = option.get('selected');
    });

    return columnIds;
  }),

  _selectObserver: observer('filteredOptions.@each.selected', function () {
    var selectedCount = 0;
    this.filteredOptions.forEach(function (option) {
      if(option.selected) {
        selectedCount++;
      }
    });
    this.set('selectAll', selectedCount > 0 && selectedCount === this.get('filteredOptions.length'));
  }),

  selectAllAction: action(function (checked) {
    this.filteredOptions.forEach(function (option) {
      set(option, 'selected', checked);
    });
  }),
  mycloseModal: action(function () {
    this.targetObject.send("closeModal");
  }),
  ok: action(function () {
    this.targetObject.send("columnsSelected", this.selectedColumnIDs);
    this.targetObject.send("closeModal");
  })
});
