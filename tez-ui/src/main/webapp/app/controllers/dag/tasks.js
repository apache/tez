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

import MultiTableController from '../multi-table';
import ColumnDefinition from '../../utils/column-definition';

export default MultiTableController.extend({
  breadcrumbs: [{
    text: "All Tasks",
    routeName: "dag.tasks",
  }],

  columns: ColumnDefinition.make([{
    id: 'index',
    headerTitle: 'Task Index',
    contentPath: 'index',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      return {
        routeName: "task",
        model: row.get("entityID"),
        text: row.get("index")
      };
    }
  },{
    id: 'vertexName',
    headerTitle: 'Vertex Name',
    contentPath: 'vertexName',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      return {
        routeName: "vertex",
        model: row.get("vertexID"),
        text: row.get("vertexName")
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
    id: 'startTime',
    headerTitle: 'Start Time',
    contentPath: 'startTime',
    cellComponentName: 'date-formatter',
  },{
    id: 'endTime',
    headerTitle: 'End Time',
    contentPath: 'endTime',
    cellComponentName: 'date-formatter',
  },{
    id: 'duration',
    headerTitle: 'Duration',
    contentPath: 'duration',
    cellDefinition: {
      type: 'duration'
    }
  },{
    id: 'log',
    headerTitle: 'Successful/Last Attempt Log',
    cellComponentName: 'em-table-tasks-log-link-cell',
    enableSearch: false,
    contentPath: 'logs',
    getCellContent: function (row) {
      var attemptID = row.get("successfulAttemptID");
      if(!attemptID) {
        let allAttemptIDs = row.get("attemptIDs") || [];
        attemptID = allAttemptIDs[allAttemptIDs.length - 1];
      }
      return attemptID;
    }
  }])
});
