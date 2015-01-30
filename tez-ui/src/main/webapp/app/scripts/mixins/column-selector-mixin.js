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

  columnSelectorTitle: 'Column Selector',

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
  },

  columns: function() {
    var visibleColumnConfigs = this.get('columnConfigs').filter(function (column) {
      return this.visibleColumnIds[column.id];
    }, this);

    return App.Helpers.misc.createColumnsFromConfigs(visibleColumnConfigs);
  }.property('visibleColumnIds'),

  actions: {
    selectColumns: function () {
      var that = this;

      App.Dialogs.displayMultiSelect(this.get('columnSelectorTitle'), this.get('columnConfigs'), this.visibleColumnIds, {
        displayText: 'headerCellName'
      }).then(function (data) {
        if(isObjectsDifferent(data, that.visibleColumnIds)) {
          try {
            localStorage.setItem(that._storeKey , JSON.stringify(data));
          }catch(e){}
          that.set('visibleColumnIds', data);
        }
      });
    }
  }

});