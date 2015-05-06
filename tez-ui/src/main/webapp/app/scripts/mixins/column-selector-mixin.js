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

function isObjectsDifferent(obj1, obj2) {
  var property;
  for(property in obj1) {
    if(obj1[property] !== obj2[property]) {
      return true;
    }
  }
  for(property in obj2) {
    if(obj1[property] !== obj2[property]) {
      return true;
    }
  }
  return false;
}

App.ColumnSelectorMixin = Em.Mixin.create({

  name: 'PaginatedContentMixin',

  _storeKey: '',
  visibleColumnIds: {},
  columnConfigs: [],
  selectOptions: [],

  columnSelectorTitle: 'Column Selector',
  columnSelectorMessage: '',

  init: function(){
    var visibleColumnIds;

    this._storeKey = this.controllerName + ':visibleColumnIds';
    try {
      visibleColumnIds = JSON.parse(localStorage.getItem(this._storeKey));
    }catch(e){}

    if(!visibleColumnIds) {
      visibleColumnIds = {};

      this.get('defaultColumnConfigs').forEach(function (config) {
        visibleColumnIds[config.id] = true;
      });
    }

    this._super();
    this.set('visibleColumnIds', visibleColumnIds);
  }.observes('defaultColumnConfigs'), //To reset on entity change

  columns: function() {
    var visibleColumnConfigs = this.get('columnConfigs').filter(function (column) {
      return this.visibleColumnIds[column.id];
    }, this);

    return App.Helpers.misc.createColumnDescription(visibleColumnConfigs);
  }.property('visibleColumnIds', 'columnConfigs'),

  _getSelectOptions: function () {
    var group = null,
        highlight = false,
        visibleColumnIds = this.get('visibleColumnIds');

    return this.get('columnConfigs').map(function (config) {
      var css = '';

      highlight = highlight ^ (config.counterGroupName != group),
      group = config.counterGroupName;

      if(highlight) {
        css += ' highlight';
      }
      if(group && App.Helpers.misc.checkIOCounterGroup(group)) {
        css += ' per-io';
      }

      return Em.Object.create({
        id: config.id,
        displayText: config.headerCellName,
        css: css,
        selected: visibleColumnIds[config.id]
      });
    });
  },

  actions: {
    selectColumns: function () {
      this.set('selectOptions', this._getSelectOptions());

      Bootstrap.ModalManager.open(
        'columnSelector',
        this.get('columnSelectorTitle'),
        App.MultiSelectView.extend({
          options: this.get('selectOptions'),
          message: this.get('columnSelectorMessage')
        }),
        [Ember.Object.create({
          title: 'Ok',
          dismiss: 'modal',
          clicked: 'selectionChange'
        })],
        this
      );
    },

    selectionChange: function () {
      var visibleColumnIds = this.get('selectOptions').reduce(function (obj, option) {
            if(option.get('selected')) {
              obj[option.get('id')] = true;
            }
            return obj;
          }, {}),
          selectionToSave = this.get('selectOptions').reduce(function (obj, option) {
            var id = option.id;
            if(!id.match('_INPUT_') && !id.match('_OUTPUT_') && visibleColumnIds[id]) {
              obj[id] = true;
            }
            return obj;
          }, {});

      if(isObjectsDifferent(visibleColumnIds, this.get('visibleColumnIds'))) {
        try {
          localStorage.setItem(this._storeKey , JSON.stringify(selectionToSave));
        }catch(e){}
        this.set('visibleColumnIds', visibleColumnIds);
      }
    }
  }
});