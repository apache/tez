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

import TableController from '../table';
import ColumnDefinition from 'em-table/utils/column-definition';

var MoreObject = more.Object;

export default TableController.extend({
  searchText: "tez",

  breadcrumbs: [{
    text: "Configurations",
    routeName: "app.configs",
  }],

  columns: ColumnDefinition.make([{
    id: 'configName',
    headerTitle: 'Configuration Name',
    contentPath: 'configName',
  }, {
    id: 'configValue',
    headerTitle: 'Configuration Value',
    contentPath: 'configValue',
  }]),

  configs: Ember.computed("model.configsJSON", function () {
    var configs = JSON.parse(this.get("model.configsJSON")),
        configRows = [];

    if(configs) {
      MoreObject.forEach(configs, function (key, value) {
        configRows.push(Ember.Object.create({
          configName: key,
          configValue: value
        }));
      });
    }

    return Ember.A(configRows);
  })
});
