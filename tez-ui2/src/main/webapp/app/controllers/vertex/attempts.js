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

import TablePageController from '../table-page';
import ColumnDefinition from 'em-table/utils/column-definition';

export default TablePageController.extend({
  breadcrumbs: [{
    text: "Task Attempts",
    routeName: "vertex.attempts",
  }],

  columns: ColumnDefinition.make([{
    id: 'index',
    headerTitle: 'Attempt No',
    contentPath: 'index'
  },{
    id: 'taskIndex',
    headerTitle: 'Task Index',
    contentPath: 'taskIndex'
  },{
    id: 'status',
    headerTitle: 'Status',
    contentPath: 'status',
    cellComponentName: 'em-table-status-cell'
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
    cellDefinition: {
      type: 'date'
    }
  },{
    id: 'endTime',
    headerTitle: 'End Time',
    contentPath: 'endTime',
    cellDefinition: {
      type: 'date'
    }
  },{
    id: 'duration',
    headerTitle: 'Duration',
    contentPath: 'duration',
    cellDefinition: {
      type: 'duration'
    }
  },{
    id: 'containerID',
    headerTitle: 'Container',
    contentPath: 'containerID'
  },{
    id: 'nodeID',
    headerTitle: 'Node',
    contentPath: 'nodeID'
  }])
});
