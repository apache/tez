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

export default PageController.extend({
  queryParams: ["rowCount", "searchText", "sortColumnId", "sortOrder", "pageNo"],
  rowCount: 10,
  searchText: "",
  sortColumnId: "",
  sortOrder: "",
  pageNo: 1,

  definition: Ember.computed(function () {
    return TableDefinition.create({
      rowCount: this.get("rowCount"),
      searchText: this.get("searchText"),
      sortColumnId: this.get("sortColumnId"),
      sortOrder: this.get("sortOrder"),
      pageNo: this.get("pageNo")
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
  }
});
