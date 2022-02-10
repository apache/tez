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

import { action, computed } from '@ember/object';

import TableController from '../table';
import ColumnDefinition from '../../utils/column-definition';
import TableDefinition from '../../utils/table-definition';

export default TableController.extend({

  queryParams: ["queryID", "dagID", "appID", "user", "requestUser",
      "tablesRead", "tablesWritten", "operationID", "queue"],
  queryID: "",
  dagID: "",
  appID: "",
  executionMode: "",
  user: "",
  requestUser: "",
  tablesRead: "",
  tablesWritten: "",
  operationID: "",
  queue: "",

  // Because pageNo is a query param added by table controller, and in the current design
  // we don't want page to be a query param as only the first page will be loaded first.
  pageNum: 1,

  breadcrumbs: computed(function() {return [{
    text: "All Queries",
    routeName: "home.queries",
  }]}),

  moreAvailable: false,
  loadingMore: false,

  headerComponentNames: computed(function() {return ['queries-page-search', 'table-controls', 'pagination-ui']}),
  footerComponentNames: computed(function() {return ['pagination-ui']}),

  _definition: TableDefinition.create(),
  // Using computed, as observer won't fire if the property is not used
  definition: computed('_definition', 'appID', 'dagID', 'executionMode', 'loadingMore', 'moreAvailable', 'operationID', 'pageNum', 'queryID', 'queue', 'requestUser', 'tablesRead', 'tablesWritten', 'user', function () {

    var definition = this._definition;

    definition.setProperties({
      queryID: this.queryID,
      dagID: this.dagID,
      appID: this.appID,
      executionMode: this.executionMode,
      user: this.user,
      requestUser: this.requestUser,
      tablesRead: this.tablesRead,
      tablesWritten: this.tablesWritten,
      operationID: this.operationID,
      queue: this.queue,

      pageNum: this.pageNum,

      moreAvailable: this.moreAvailable,
      loadingMore: this.loadingMore
    });

    return definition;
  }),

  columns: ColumnDefinition.make([{
    id: 'entityID',
    headerTitle: 'Query ID',
    contentPath: 'entityID',
    cellComponentName: 'em-table-linked-cell',
    minWidth: "250px",
    getCellContent: function (row) {
      return {
        routeName: "query",
        model: row.get("entityID"),
        text: row.get("entityID")
      };
    }
  },{
    id: 'requestUser',
    headerTitle: 'User',
    contentPath: 'requestUser',
    minWidth: "100px",
  },{
    id: 'status',
    headerTitle: 'Status',
    contentPath: 'status',
    cellComponentName: 'em-table-status-cell',
    minWidth: "105px",
  },{
    id: 'queryText',
    headerTitle: 'Query',
    contentPath: 'queryText',
  },{
    id: 'dagID',
    headerTitle: 'DAG ID',
    contentPath: 'dagID',
    cellComponentName: 'em-table-linked-cell',
    minWidth: "250px",
    getCellContent: function (row) {
      var dagID = row.get("dagID") || row.get("dag.firstObject.entityID");
      return {
        routeName: "dag",
        model: dagID,
        text: dagID
      };
    }
  },{
    id: 'tablesRead',
    headerTitle: 'Tables Read',
    contentPath: 'tablesRead',
  },{
    id: 'tablesWritten',
    headerTitle: 'Tables Written',
    contentPath: 'tablesWritten',
  },{
    id: 'llapAppID',
    headerTitle: 'LLAP App ID',
    contentPath: 'llapAppID',
    minWidth: "250px",
  },{
    id: 'clientAddress',
    headerTitle: 'Client Address',
    contentPath: 'clientAddress',
    hiddenByDefault: true,
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
    id: 'appID',
    headerTitle: 'Application Id',
    contentPath: 'appID',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      var appID = row.get("appID") || row.get("dag.firstObject.appID");
      return {
        routeName: "app",
        model: appID,
        text: appID
      };
    }
  },{
    id: 'queue',
    headerTitle: 'Queue',
    contentPath: 'queue',
  },{
    id: 'executionMode',
    headerTitle: 'Execution Mode',
    contentPath: 'executionMode',
    minWidth: "100px",
  },{
    id: 'hiveAddress',
    headerTitle: 'Hive Server 2 Address',
    contentPath: 'hiveAddress',
    hiddenByDefault: true,
  },{
    id: 'instanceType',
    headerTitle: 'Client Type',
    contentPath: 'instanceType',
    minWidth: "100px",
  }]),

  getCounterColumns: function () {
    return [];
  },

  search: action(function (properties) {
    this.setProperties(properties);
  }),
  pageChanged: action(function (pageNum) {
    this.set("pageNum", pageNum);
  })
});
