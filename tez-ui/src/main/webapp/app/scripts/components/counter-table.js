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

App.CounterTableComponent = Em.Component.extend({
  layoutName: 'components/counter-table',
  nameFilter: null,

  validFilter: function () {
    try {
      new RegExp(this.get('nameFilter'), 'i');
      return true;
    }
    catch(e){}
    return false;
  }.property('nameFilter'),

  filteredData: function() {
    var rawData = this.get('data') || [];
    if (Em.isEmpty(this.nameFilter) || !this.get('validFilter')) {
      return rawData;
    }

    var filtered = [],
        filterStringRegex = new RegExp(this.nameFilter, 'i');

    rawData.forEach(function(cg) {
      var tmpcg = {
        counterGroupName: cg['counterGroupName'],
        counterGroupDisplayName: cg['counterGroupDisplayName'],
        counters: []
      };

      var counters = cg['counters'] || [];
      counters.forEach(function(counter) {
        if (filterStringRegex.test(counter['counterDisplayName'])) {
          tmpcg.counters.push(counter);
        }
      });

      // show counter groups only if filter match is not empty.
      if (tmpcg.counters.length > 0) {
        filtered.push(tmpcg);
      }
    })

    return filtered;
  }.property('data', 'nameFilter', 'timeStamp')
});

Em.Handlebars.helper('counter-table-component', App.CounterTableComponent);
