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

import isIOCounter from '../utils/misc';

export default Ember.Component.extend({

  classNames: ['column-selector'],

  searchText: "",
  selectAll: false,

  content: null,

  options: Ember.computed("content.columns", "content.visibleColumnIDs", function () {
    var group,
        highlight = false,
        visibleColumnIDs = this.get('content.visibleColumnIDs') || {};

    return this.get('content.columns').map(function (definition) {
      var css = '';

      highlight = highlight ^ (Ember.get(definition, "counterGroupName") !== group);
      group = Ember.get(definition, "counterGroupName");

      if(highlight) {
        css += ' highlight';
      }
      if(group && isIOCounter(group)) {
        css += ' per-io';
      }

      return Ember.Object.create({
        id: Ember.get(definition, "id"),
        displayText: Ember.get(definition, "headerTitle"),
        css: css,
        selected: visibleColumnIDs[Ember.get(definition, "id")]
      });
    });
  }),

  filteredOptions: Ember.computed("options", "searchText", function () {
    var options = this.get('options'),
        searchText = this.get('searchText');

    if (!searchText) {
      return options;
    }

    return options.filter(function (option) {
      return option.get('displayText').match(searchText);
    });
  }),

  selectedColumnIDs: Ember.computed("options", function () {
    var columnIds = {};
    this.get('options').forEach(function (option) {
      columnIds[option.get("id")] = option.get('selected');
    });

    return columnIds;
  }),

  _selectObserver: Ember.observer('filteredOptions.@each.selected', function () {
    var selectedCount = 0;
    this.get('filteredOptions').forEach(function (option) {
      if(Ember.get(option, 'selected')) {
        selectedCount++;
      }
    });
    this.set('selectAll', selectedCount > 0 && selectedCount === this.get('filteredOptions.length'));
  }),

  actions: {
    selectAll: function (checked) {
      this.get('filteredOptions').forEach(function (option) {
        Ember.set(option, 'selected', checked);
      });
    },
    closeModal: function () {
      this.get("targetObject").send("closeModal");
    },
    ok: function () {
      this.get("targetObject").send("columnsSelected", this.get("selectedColumnIDs"));
    }
  }
});
