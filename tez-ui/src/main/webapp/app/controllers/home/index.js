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

  queryParams: ["dagName", "dagID", "submitter", "status", "appID", "callerID", "appid", "id", "user", "dag_name"],
  dagName: "",
  dagID: "",
  submitter: "",
  status: "",
  appID: "",
  callerID: "",
  appid: "",
  id: "",
  user: "",
  dag_name: "",

  // Because pageNo is a query param added by table controller, and in the current design
  // we don't want page to be a query param as only the first page will be loaded first.
  pageNum: 1,

  breadcrumbs: [{
    text: "All DAGs",
    routeName: "home.index",
  }],

  moreAvailable: false,
  loadingMore: false,

  headerComponentNames: ['dags-page-search', 'table-controls', 'pagination-ui'],
  footerComponentNames: ['pagination-ui'],

  _definition: TableDefinition.create(),
  // Using computed, as observer won't fire if the property is not used
  definition: Ember.computed("dagName", "dagID", "submitter", "status",
      "appID", "callerID", "pageNum", "moreAvailable", "loadingMore", function () {

    var definition = this.get("_definition");
    if (!this.get("appID")) {
      this.set("appID", this.get("appid"));
      this.set("appid", "");
    }
    if (!this.get("dagID")) {
      this.set("dagID", this.get("id"));
      this.set("id", "");
    }
    if (!this.get("submitter")) {
      this.set("submitter", this.get("user"));
      this.set("user", "");
    }
    if (!this.get("dagName")) {
      this.set("dagName", this.get("dag_name"));
      this.set("dag_name", "");
    }

    definition.setProperties({
      dagName: this.get("dagName"),
      dagID: this.get("dagID"),
      submitter: this.get("submitter"),
      status: this.get("status"),
      appID: this.get("appID"),
      callerID: this.get("callerID"),

      pageNum: this.get("pageNum"),

      moreAvailable: this.get("moreAvailable"),
      loadingMore: this.get("loadingMore")
    });

    return definition;
  }),

  columns: ColumnDefinition.make([{
    id: 'name',
    headerTitle: 'Dag Name',
    contentPath: 'name',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      return {
        routeName: "dag",
        model: row.get("entityID"),
        text: row.get("name")
      };
    }
  },{
    id: 'entityID',
    headerTitle: 'Id',
    contentPath: 'entityID'
  },{
    id: 'submitter',
    headerTitle: 'Submitter',
    contentPath: 'submitter'
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
    id: 'appID',
    headerTitle: 'Application Id',
    contentPath: 'appID',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      return {
        routeName: "app",
        model: row.get("appID"),
        text: row.get("appID")
      };
    }
  },{
    id: 'queue',
    headerTitle: 'Queue',
    contentPath: 'queue'
  },{
    id: 'callerID',
    headerTitle: 'Caller ID',
    contentPath: 'callerID'
  },{
    id: 'callerContext',
    headerTitle: 'Caller Context',
    contentPath: 'callerContext'
  },{
    id: 'logs',
    headerTitle: 'Logs',
    contentPath: 'containerLogs',
    cellComponentName: "em-table-linked-cell",
    cellDefinition: {
      target: "_blank"
    }
  }]),

  getCounterColumns: function () {
    return this._super().concat(this.get('env.app.tables.defaultColumns.dagCounters'));
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
