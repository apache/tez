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

App.TezAppConfigsController = Em.ObjectController.extend(App.PaginatedContentMixin, {
  needs: "tezApp",
  count: 10,

  key: null,
  value: null,

  loadEntities: function() {
    var that = this,
    count = 0,
    filter = this.getFilterProperties(),
    configs = configs = this.get('configs').content,
    filtered = [],
    i = 0;

    if(filter.fromId) {
      while(i < configs.length && configs[i].id != filter.fromId) {
        i++;
      }
    }

    // Filter the available data
    for(; i < configs.length && filtered.length < filter.limit; i++){
      if((that.key === null || configs[i].get('key').indexOf(that.key) !=-1) && (that.value === null || configs[i].get('value').indexOf(that.value) != -1)) {
        filtered.push(configs[i]);
      }
    }

    this.set('entities', filtered);
    this.set('loading', false);
  },

  loadData: function() {
    var filters = {
      primary: {
        key: this.key,
        value: this.value
      },
    };
    this.setFiltersAndLoadEntities(filters);
  },

  actions : {
    filterUpdated: function(filterID, value) {
      // any validations required goes here.
      if (!!value) {
        this.set(filterID, value);
      } else {
        this.set(filterID, null);
      }
      this.loadData();
    }
  },

  columns: function() {
    var columnHelper = function(displayText, columnId) {
      return App.ExTable.ColumnDefinition.createWithMixins(App.ExTable.FilterColumnMixin, {
        textAlign: 'text-align-left',
        headerCellName: displayText,
        filterID: columnId,
        contentPath: columnId
      });
    }

    return [
        columnHelper("Conf key", 'key'),
        columnHelper("Conf value", 'value'),
    ];
  }.property(),

});