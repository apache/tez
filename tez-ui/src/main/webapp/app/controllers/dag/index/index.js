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

import MultiTableController from '../../multi-table';
import ColumnDefinition from 'em-table/utils/column-definition';

export default MultiTableController.extend({
  columns: ColumnDefinition.make([{
    id: 'name',
    headerTitle: 'Vertex Name',
    contentPath: 'name',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      return {
        routeName: "vertex",
        model: row.get("entityID"),
        text: row.get("name")
      };
    }
  },{
    id: 'status',
    headerTitle: 'Status',
    contentPath: 'status',
    cellComponentName: 'em-table-status-cell',
    observePath: true
  },{
    id: 'progress',
    headerTitle: 'Progress',
    contentPath: 'progress',
    cellComponentName: 'em-table-progress-cell',
    observePath: true
  },{
    id: 'totalTasks',
    headerTitle: 'Total Tasks',
    contentPath: 'totalTasks',
    observePath: true
  },{
    id: 'succeededTasks',
    headerTitle: 'Succeeded Tasks',
    contentPath: 'succeededTasks',
    observePath: true
  },{
    id: 'runningTasks',
    headerTitle: 'Running Tasks',
    contentPath: 'runningTasks',
    observePath: true
  },{
    id: 'pendingTasks',
    headerTitle: 'Pending Tasks',
    contentPath: 'pendingTasks',
    observePath: true
  },{
    id: 'failedTaskAttempts',
    headerTitle: 'Failed Task Attempts',
    contentPath: 'failedTaskAttempts',
    observePath: true
  },{
    id: 'killedTaskAttempts',
    headerTitle: 'Killed Task Attempts',
    contentPath: 'killedTaskAttempts',
    observePath: true
  }]),

  definition: Ember.computed("model", function () {
    var definition = this._super();
    definition.set("recordType", "vertex");
    return definition;
  }),

  stats: Ember.computed("model.@each.loadTime", function () {
    var vertices = this.get("model");

    if(vertices) {
      let succeededVertices = 0,
          succeededTasks = 0,
          totalTasks =0,

          failedTasks = 0,
          killedTasks = 0,
          failedTaskAttempts = 0,
          killedTaskAttempts = 0;

      vertices.forEach(function (vertex) {
        if(vertex.get("status") === "SUCCEEDED") {
          succeededVertices++;
        }

        succeededTasks += vertex.get("succeededTasks");
        totalTasks += vertex.get("totalTasks");

        failedTasks += vertex.get("failedTasks");
        killedTasks += vertex.get("killedTasks");

        failedTaskAttempts += vertex.get("failedTaskAttempts");
        killedTaskAttempts += vertex.get("killedTaskAttempts");
      });

      return {
        succeededVertices: succeededVertices,
        totalVertices: vertices.get("length"),

        succeededTasks: succeededTasks,
        totalTasks: totalTasks,

        failedTasks: failedTasks,
        killedTasks: killedTasks,

        failedTaskAttempts: failedTaskAttempts,
        killedTaskAttempts: killedTaskAttempts
      };
    }
  }),

  beforeSort: function () {
    return true;
  }

});
