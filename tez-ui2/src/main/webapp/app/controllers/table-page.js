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

import PageController from './page';
import TableDefinition from 'em-table/utils/table-definition';
import isIOCounter from '../utils/misc';

var MoreObject = more.Object;

export default PageController.extend({
  queryParams: ["rowCount", "searchText", "sortColumnId", "sortOrder", "pageNo"],
  rowCount: 10,
  searchText: "",
  sortColumnId: "",
  sortOrder: "",
  pageNo: 1,

  headerComponentNames: ['em-table-search-ui', 'table-controls', 'em-table-pagination-ui'],

  visibleColumnIDs: {},
  columnSelectorTitle: 'Column Selector',
  columnSelectorMessage: "",

  definition: Ember.computed(function () {
    return TableDefinition.create({
      rowCount: this.get("rowCount"),
      searchText: this.get("searchText"),
      sortColumnId: this.get("sortColumnId"),
      sortOrder: this.get("sortOrder"),
      pageNo: this.get("pageNo")
    });
  }),

  storageID: Ember.computed("name", function () {
    return this.get("name") + ":visibleColumnIDs";
  }),

  initVisibleColumns: Ember.on("init", Ember.observer("columns", function () { //To reset on entity change
    var visibleColumnIDs = this.get("localStorage").get(this.get("storageID")) || {};

    this.get('columns').forEach(function (config) {
      if(visibleColumnIDs[config.id] !== false) {
        visibleColumnIDs[config.id] = true;
      }
    });

    this.set('visibleColumnIDs', visibleColumnIDs);
  })),

  visibleColumns: Ember.computed('visibleColumnIDs', 'columns', function() {
    var visibleColumnIDs = this.visibleColumnIDs;
    return this.get('columns').filter(function (column) {
      return visibleColumnIDs[column.get("id")];
    });
  }),

  actions: {
    searchChanged: function (searchText) {
      this.set("searchText", searchText);
    },
    sortChanged: function (sortColumnId, sortOrder) {
      this.setProperties({
        sortColumnId,
        sortOrder
      });
    },
    rowsChanged: function (rowCount) {
      // Change to rows action in em-table
      this.set("rowCount", rowCount);
    },
    pageChanged: function (pageNum) {
      this.set("pageNo", pageNum);
    },

    // Column selection actions
    openColumnSelector: function () {
      this.send("openModal", "column-selector", {
        title: this.get('columnSelectorTitle'),
        targetObject: this,
        content: {
          message: this.get('columnSelectorMessage'),
          columns: this.get('columns'),
          visibleColumnIDs: this.get('visibleColumnIDs')
        }
      });
    },
    columnsSelected: function (visibleColumnIDs) {
      var columnIDs = {};

      MoreObject.forEach(visibleColumnIDs, function (key, value) {
        if(!isIOCounter(key)) {
          columnIDs[key] = value;
        }
      });

      if(!MoreObject.equals(columnIDs, this.get("visibleColumnIDs"))) {
        this.get("localStorage").set(this.get("storageID"), columnIDs);
        this.set('visibleColumnIDs', columnIDs);
      }
    }
  }
});
