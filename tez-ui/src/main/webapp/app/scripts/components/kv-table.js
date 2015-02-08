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

App.KvTableComponent = Em.Component.extend({
  layoutName: 'components/kv-table',
  filterExp: null,
  showAllButtonClass: '',
  errorMsgClass: '',

  actions: {
    showAllButtonClicked: function() {
      this.set('filterExp', null);
    }
  },

  showError: function(show) {
    this.set('errorMsgClass', show ? '' : 'no-display');
  },

  filteredKVs: function() {
    var filterExp = this.get('filterExp');
    var kvList = this.get('data') || [],
        filteredKvs = [],
        filterStringRegex;

    if (filterExp) {
      this.set('showAllButtonClass', '');
    } else {
      this.set('showAllButtonClass', 'hidden');
    }

    try {
      filterStringRegex = new RegExp(filterExp, 'i');
    } catch(e) {
      this.showError(true);
      Em.Logger.debug("Invalid regex " + e);
      return;
    }

    this.showError(false);
    if (Em.isEmpty(filterExp)) {
      return kvList;
    }

    kvList.forEach(function (kv) {
      if (filterStringRegex.test(kv.get('key')) || filterStringRegex.test(kv.get('value'))) {
        filteredKvs.push(kv);
      }
    });

    return filteredKvs;
  }.property('data', 'filterExp')
});

Em.Handlebars.helper('kv-table-component', App.KvTableComponent);
