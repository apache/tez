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

App.BasicTableComponent = Em.Component.extend({
  layoutName: 'components/basic-table',

  sortColumnId: '',
  sortOrder: '',

  searchText: '',
  searchRegEx: null,
  searchColumnNames: null,

  statusMessage: null,

  isSorting: false,
  isSearching: false,

  pageNum: 1,
  rowCount: 10,
  rowCountOptions: [5, 10, 25, 50, 100],
  pageNavOnFooterAt: 25,

  _sortedRows: null,

  init: function () {
    this._super();
    if(this.get('searchText')) {
      this._searchObserver();
    }
    this._sortObserver();
  },

  totalPages: function () {
    return Math.ceil(this.get('_searchedRows.length') / this.get('rowCount'));
  }.property('_searchedRows.length', 'rowCount'),

  hasPageNavOnFooter: function () {
    return this.get('enablePagination') && this.get('_rows.length') >= this.get('pageNavOnFooterAt');
  }.property('enablePagination', '_rows.length', 'pageNavOnFooterAt'),

  _showHeader: function () {
    return this.get('enableSearch') ||
        this.get('enablePagination') ||
        this.get('extraHeaderItem') ||
        this.get('_statusMessage');
  }.property('enableSearch', 'enablePagination', 'extraHeaderItem', '_statusMessage'),

  _statusMessage: function() {
    if(this.get('enableStatus') == false) {
      return null;
    }
    if(this.get('isSorting')) {
      return "Sorting...";
    }
    else if(this.get('isSearching')) {
      return "Searching...";
    }
    return this.get('statusMessage');
  }.property('isSearching', 'isSorting', 'statusMessage', 'enableStatus'),

  _pageNumResetObserver: function () {
    this.set('pageNum', 1);
  }.observes('searchRegEx', 'rowCount'),

  _searchedRows: function () {
    var regex = this.get('searchRegEx'),
        rows = this.get('_sortedRows') || [],
        searchColumnNames,
        columns;

    function checkRow(column) {
      var value;
      if(!column.get('searchAndSortable')) {
        return false;
      }
      value = column.getSearchValue(this);
      return (typeof value == 'string') ? value.match(regex) : false;
    }

    this.set('isSearching', false);

    if(!regex) {
      return rows;
    }
    else {
      searchColumnNames = this.get('searchColumnNames'),
      columns = searchColumnNames ? this.get('columns').filter(function (column) {
        return searchColumnNames.indexOf(column.get('headerCellName')) != -1;
      }) : this.get('columns');

      return rows.filter(function (row) {
        return columns.some(checkRow, row);
      });
    }
  }.property('columns.@each', '_sortedRows.@each', 'searchRegEx'),

  _columns: function () {
    var columns = this.get('columns'),
        widthPercentageToFit = 100 / columns.length;

      columns.map(function (column) {
        var templateName = column.get('templateName'),
            cellOptions = {
              column: column
            };

        if(templateName) {
          cellOptions.templateName = templateName;
        }

        column.setProperties({
          width: widthPercentageToFit + "%",
          cellView: App.BasicTableComponent.CellView.extend(cellOptions),
          headerCellView: App.BasicTableComponent.HeaderCellView.extend({
            column: column,
            table: this
          })
        });
      });

    return columns;
  }.property('columns'),

  _rows: function () {
    var startIndex = (this.get('pageNum') - 1) * this.get('rowCount'),
        rows = this.get('_searchedRows').slice(startIndex, startIndex + this.get('rowCount'));
    this.sendAction('rowsChanged', rows);
    return rows;
  }.property('_searchedRows.@each', 'rowCount', 'pageNum'),

  _searchObserver: function () {
    var searchText = this.get('searchText'),
        columnNames = [],
        delimIndex,
        rowsCount,
        that = this;

    if(searchText) {
      delimIndex = searchText.indexOf(':');
      if(delimIndex != -1) {
        columnNames = searchText.substr(0, delimIndex).
          split(",").
          reduce(function (arr, columnName) {
            columnName = columnName.trim();
            if(columnName) {
              arr.push(columnName);
            }
            return arr;
          }, []);
        searchText = searchText.substr(delimIndex + 1);
      }
      searchText = searchText.trim();
    }

    rowsCount = this.get('rows.length');

    if(rowsCount) {
      this.set('isSearching', true);

      Ember.run.later(function () {
        that.setProperties({
          searchRegEx: searchText ? new RegExp(searchText, 'im') : null,
          searchColumnNames: columnNames.length ? columnNames : null
        });
      }, 400);
    }
  }.observes('searchText'),

  _sortObserver: function () {
    var rows = this.get('rows'),
        column = this.get('columns').findBy('id', this.get('sortColumnId')),
        ascending = this.get('sortOrder') == 'asc',
        that = this;

    if(rows && rows.get('length') > 0 && column) {
      this.set('isSorting', true);

      Ember.run.later(function () {
        /*
         * Creating sortArray as calling getSortValue form inside the
         * sort function would be more costly.
         */
        var sortArray = rows.map(function (row) {
          return {
            value: column.getSortValue(row),
            row: row
          };
        });

        sortArray.sort(function (a, b){
          if(ascending ^ (a.value > b.value)) {
            return -1;
          }
          else if(ascending ^ (a.value < b.value)) {
            return 1;
          }
          return 0;
        });

        that.setProperties({
          _sortedRows: sortArray.map(function (record) {
            return record.row;
          }),
          isSorting: false
        });

      }, 400);
    }
    else {
      this.set('_sortedRows', rows);
    }
  }.observes('sortColumnId', 'sortOrder', 'rows.@each'),

  actions: {
    search: function (searchText) {
      this.set('searchText', searchText);
    },
    sort: function (columnId) {
      if(this.get('sortColumnId') != columnId) {
        this.setProperties({
          sortColumnId: columnId,
          sortOrder: 'asc'
        });
      }
      else {
        this.set('sortOrder', this.get('sortOrder') == 'asc' ? 'desc' : 'asc');
      }
    },

    changePage: function (pageNum) {
      this.set('pageNum', pageNum);
    }
  }
});

Em.Handlebars.helper('basic-table-component', App.BasicTableComponent);
