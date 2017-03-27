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
import TableDefinition from 'em-table/utils/table-definition';

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

  breadcrumbs: [{
    text: "All Queries",
    routeName: "home.queries",
  }],

  moreAvailable: false,
  loadingMore: false,

  headerComponentNames: ['queries-page-search', 'table-controls', 'pagination-ui'],
  footerComponentNames: ['pagination-ui'],

  _definition: TableDefinition.create(),
  // Using computed, as observer won't fire if the property is not used
  definition: Ember.computed("queryID", "dagID", "appID", "user", "requestUser",
      "executionMode", "tablesRead", "tablesWritten", "operationID", "queue",
      "pageNum", "moreAvailable", "loadingMore", function () {

    var definition = this.get("_definition");

    definition.setProperties({
      queryID: this.get("queryID"),
      dagID: this.get("dagID"),
      appID: this.get("appID"),
      executionMode: this.get("executionMode"),
      user: this.get("user"),
      requestUser: this.get("requestUser"),
      tablesRead: this.get("tablesRead"),
      tablesWritten: this.get("tablesWritten"),
      operationID: this.get("operationID"),
      queue: this.get("queue"),

      pageNum: this.get("pageNum"),

      moreAvailable: this.get("moreAvailable"),
      loadingMore: this.get("loadingMore")
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
    contentPath: 'dag.firstObject.entityID',
    cellComponentName: 'em-table-linked-cell',
    minWidth: "250px",
    getCellContent: function (row) {
      return {
        routeName: "dag",
        model: row.get("dag.firstObject.entityID"),
        text: row.get("dag.firstObject.entityID")
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
    contentPath: 'dag.firstObject.appID',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      return {
        routeName: "app",
        model: row.get("dag.firstObject.appID"),
        text: row.get("dag.firstObject.appID")
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

  actions: {
    search: function (properties) {
      this.setProperties(properties);
    },
    pageChanged: function (pageNum) {
      this.set("pageNum", pageNum);
    },
  }

});
