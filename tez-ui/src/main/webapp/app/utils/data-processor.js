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

import EmberObject, { computed, observer } from '@ember/object';
import { alias } from '@ember/object/computed';
import { on } from '@ember/object/evented';
import { later, once } from '@ember/runloop';

import SQL from './sql';

/**
 * Handles Sorting, Searching & Pagination
 */
export default EmberObject.extend({
  isSorting: false,
  isSearching: false,

  tableDefinition: null,

  sql: SQL.create(),

  rows: [],
  _sortedRows: [],
  _searchedRows: [],
  _facetFilteredRows: [],

  _searchObserver: on("init", observer('tableDefinition.searchText', 'tableDefinition._actualSearchType', '_sortedRows.[]', function () {
    once(this, "startSearch");
  })),

  _sortObserver: on("init", observer(
    'tableDefinition.sortColumnId',
    'tableDefinition.sortOrder',
    'rows.[]', function () {
      once(this, "startSort");
  })),

  _facetedFilterObserver: on("init", observer('tableDefinition.facetConditions', '_searchedRows.[]', function () {
    once(this, "startFacetedFilter");
  })),

  regexSearch: function (clause, rows, columns) {
    var regex;

    try {
      regex = new RegExp(clause, "i");
    }
    catch(e) {
      regex = new RegExp("", "i");
    }

    function checkRow(column) {
      var value;
      if(!column.get('enableSearch')) {
        return false;
      }
      value = column.getSearchValue(this);

      if(typeof value === 'string') {
        value = value.toLowerCase();
        return value.match(regex);
      }

      return false;
    }

    return rows.filter(function (row) {
      return columns.some(checkRow, row);
    });
  },

  startSearch: function () {
    var searchText = String(this.get('tableDefinition.searchText')),
        rows = this._sortedRows || [],
        columns = this.get('tableDefinition.columns'),
        actualSearchType = this.get('tableDefinition._actualSearchType'),
        that = this;

    if(searchText) {
      this.set("isSearching", true);

      later(function () {
        var result;

        switch(actualSearchType) {
          case "SQL":
            result = that.get("sql").search(searchText, rows, columns);
            break;

          //case "Regex": Commenting as default will be called anyways
          default:
            result = that.regexSearch(searchText, rows, columns);
            break;
        }

        that.setProperties({
          _searchedRows: result,
          isSearching: false
        });
      });
    }
    else {
      this.set("_searchedRows", rows);
    }
  },

  compareFunction: function (a, b){
    // Checking for undefined and null to handle some special cases in JavaScript comparison
    // Eg: 1 > undefined = false & 1 < undefined = false
    // "a1" > null = false & "a1" < null = false
    if(a === undefined || a === null) {
      return -1;
    }
    else if(b === undefined || b === null) {
      return 1;
    }
    else if(a < b) {
      return -1;
    }
    else if(a > b) {
      return 1;
    }
    else {
      return 0;
    }
  },

  startSort: function () {
    var rows = this.rows,
        tableDefinition = this.tableDefinition,
        sortColumnId = this.get('tableDefinition.sortColumnId'),
        descending = this.get('tableDefinition.sortOrder') === 'desc',
        that = this,
        column;

    if(tableDefinition) {
      column = tableDefinition.get('columns').find(function (element) {
        return element.get('id') === sortColumnId;
      });
    }

    if(rows && Array.isArray(rows.content)) {
      rows = rows.toArray();
    }

    if(rows && rows.get('length') > 0 && column) {
      this.set('isSorting', true);

      later(function () {
        /*
         * Creating sortArray as calling getSortValue form inside the
         * sort function every time would be more costly.
         */
        var sortArray = rows.map(function (row) {
          return {
            value: column.getSortValue(row),
            row: row
          };
        }),
        compareFunction = that.get("compareFunction");

        sortArray.sort(function (a, b) {
          var result = compareFunction(a.value, b.value);
          if(descending && result) {
            result = -result;
          }
          return result;
        });

        that.setProperties({
          _sortedRows: sortArray.map(function (record) {
            return record.row;
          }),
          isSorting: false
        });
      });
    }
    else {
      this.set('_sortedRows', rows);
    }
  },

  startFacetedFilter: function () {
    var clause = this.sql.createFacetClause(this.get('tableDefinition.facetConditions'), this.get("tableDefinition.columns")),
        rows = this._searchedRows || [],
        columns = this.get('tableDefinition.columns'),
        that = this;

    if(clause && columns) {
      this.set("isSearching", true);

      later(function () {
        var result = that.get("sql").search(clause, rows, columns);

        that.setProperties({
          _facetFilteredRows: result,
          isSearching: false
        });
      });
    }
    else {
      this.set("_facetFilteredRows", rows);
    }
  },

  facetedFields: computed('_searchedRows.[]', 'tableDefinition.columns', function () {
    var searchedRows = this._searchedRows,
        columns = this.get('tableDefinition.columns'),
        fields = [];

    if(columns) {
      columns.forEach(function (column) {
        var facetedData;
        if(column.facetType) {
          facetedData = column.facetType.facetRows(column, searchedRows);
          if(facetedData) {
            fields.push({
              column: column,
              facets: facetedData
            });
          }
        }
      });
    }

    return fields;
  }),

  pageDetails: computed("tableDefinition.rowCount", "tableDefinition.pageNum", "_facetFilteredRows.length", function () {
    var tableDefinition = this.tableDefinition,

        pageNum = tableDefinition.get('pageNum'),
        rowCount =  tableDefinition.get('rowCount'),

        startIndex = (pageNum - 1) * rowCount,

        totalRecords = this.get('_facetFilteredRows.length');

    if(startIndex < 0) {
      startIndex = 0;
    }

    return {
      pageNum: pageNum,
      totalPages: Math.ceil(totalRecords / rowCount),
      rowCount: rowCount,

      startIndex: startIndex,

      fromRecord: totalRecords ? startIndex + 1 : 0,
      toRecord: Math.min(startIndex + rowCount, totalRecords),
      totalRecords: totalRecords
    };
  }),
  totalPages: alias("pageDetails.totalPages"), // Adding an alias for backward compatibility

  // Paginate
  processedRows: computed('_facetFilteredRows.[]', 'tableDefinition.rowCount', 'tableDefinition.pageNum', function () {
    var rowCount =  this.get('tableDefinition.rowCount'),
        startIndex = (this.get('tableDefinition.pageNum') - 1) * rowCount;
    return this._facetFilteredRows.slice(startIndex, startIndex + rowCount);
  }),
});
