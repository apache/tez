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

import { action, computed, observer } from '@ember/object';
import { on } from '@ember/object/evented';
import { inject as service } from '@ember/service';
import MoreObject from '../utils/more-object';

import AbstractController from './abstract';
import TableDefinition from '../utils/table-definition';
import isIOCounter from '../utils/misc';

import CounterColumnDefinition from '../utils/counter-column-definition';

export default AbstractController.extend({
  queryParams: ["rowCount", "searchText", "sortColumnId", "sortOrder", "pageNo"],
  rowCount: 10,
  searchText: "",
  sortColumnId: "",
  sortOrder: "",
  pageNo: 1,

  columns: [],

  headerComponentNames: ['em-table-search-ui', 'table-controls', 'em-table-pagination-ui'],

  visibleColumnIDs: {},
  columnSelectorTitle: 'Column Selector',
  columnSelectorMessage: "",

  polling: service("pollster"),

  definition: computed('model', 'pageNo', 'rowCount', 'searchText', 'sortColumnId', 'sortOrder', function () {
    return TableDefinition.create({
      rowCount: this.rowCount,
      searchText: this.searchText,
      sortColumnId: this.sortColumnId,
      sortOrder: this.sortOrder,
      pageNo: this.pageNo,
      headerAsSortButton: true,
    });
  }),

  storageID: computed("name", function () {
    return this.name + ":visibleColumnIDs";
  }),

  initVisibleColumns: on("init", observer("columns", function () { //To reset on entity change
    var visibleColumnIDs = this.localStorage.get(this.storageID) || {};

    this.columns.forEach(function (config) {
      if(visibleColumnIDs[config.id] === undefined) {
        visibleColumnIDs[config.id] = !config.hiddenByDefault;
      }
    });

    this.set('visibleColumnIDs', visibleColumnIDs);
  })),

  beforeSort: function (columnDefinition) {
    if(this.get("polling.isReady")) {
      let columnName = columnDefinition.get("headerTitle");
      switch(columnDefinition.get("contentPath")) {
        case "counterGroupsHash":
          columnName = "Counters";
          /* falls through */
        case "status":
        case "progress":
          this.send("openModal", {
            title: "Cannot sort!",
            content: `Sorting on ${columnName} is disabled for running DAGs while Auto Refresh is enabled!`
          });
          return false;
      }
    }
    return true;
  },

  allColumns: computed('beforeSort', 'columns', function () {
    var columns = this.columns,
        counters = this.getCounterColumns(),
        beforeSort = this.beforeSort.bind(this);

    columns = columns.concat(CounterColumnDefinition.make(counters));

    columns.forEach(function (column) {
      column.set("beforeSort", beforeSort);
    });

    return columns;
  }),

  visibleColumns: computed('visibleColumnIDs', 'allColumns', function() {
    var visibleColumnIDs = this.visibleColumnIDs;
    return this.allColumns.filter(function (column) {
      return visibleColumnIDs[column.get("id")];
    });
  }),

  getCounterColumns: function () {
    return this.get('env.app.tables.defaultColumns.counters');
  },

  searchChanged: action(function (searchText) {
    this.set("searchText", searchText);
  }),
  sortChanged: action(function (sortColumnId, sortOrder) {
    this.setProperties({
      sortColumnId,
      sortOrder
    });
  }),
  rowCountChanged: action(function (rowCount) {
    this.set("rowCount", rowCount);
  }),
  pageChanged: action(function (pageNum) {
    this.set("pageNo", pageNum);
  }),

  rowsChanged: action(function (rows) {
    this.send("setPollingRecords", rows);
  }),

  // Column selection actions
  openColumnSelector: action(function () {
    this.send("openModal", "column-selector", {
      title: this.columnSelectorTitle,
      targetObject: this,
      content: {
        message: this.columnSelectorMessage,
        columns: this.allColumns,
        visibleColumnIDs: this.visibleColumnIDs
      }
    });
  }),
  columnsSelected: action(function (visibleColumnIDs) {
    var columnIDs = {};

    MoreObject.forEach(visibleColumnIDs, function (key, value) {
      if(!isIOCounter(key)) {
        columnIDs[key] = value;
      }
    });

    if(!MoreObject.equals(columnIDs, this.visibleColumnIDs)) {
      this.localStorage.set(this.storageID, columnIDs);
      this.set('visibleColumnIDs', columnIDs);
    }
  })
});
