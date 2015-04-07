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

  statusMessage: null,
  internalStatusMessage: null,

  pageNum: 1,
  rowCount: 10,
  rowCountOptions: [5, 10, 25, 50, 100],
  pageNavOnFooterAt: 25,

  totalPages: function () {
    return Math.ceil(this.get('rows.length') / this.get('rowCount'));
  }.property('rows.length', 'rowCount'),

  hasPageNavOnFooter: function () {
    return this.get('enablePagination') && this.get('_rows.length') >= this.get('pageNavOnFooterAt');
  }.property('enablePagination', '_rows.length', 'pageNavOnFooterAt'),

  _showHeader: function () {
    return this.get('enablePagination') ||
        this.get('extraHeaderItem') ||
        this.get('_statusMessage');
  }.property('enablePagination', 'extraHeaderItem', '_statusMessage'),

  _statusMessage: function() {
    return this.get('internalStatusMessage') || this.get('statusMessage');
  }.property('internalStatusMessage', 'statusMessage'),

  _pageNumResetObserver: function () {
    this.set('pageNum', 1);
  }.observes('rowCount'),

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
    var startIndex = (this.get('pageNum') - 1) * this.get('rowCount');
    return this.get('rows').slice(startIndex, startIndex + this.get('rowCount'));
  }.property('rows.@each', 'rowCount', 'pageNum'),

  actions: {
    changePage: function (pageNum) {
      this.set('pageNum', pageNum);
    }
  }
});

Em.Handlebars.helper('basic-table-component', App.BasicTableComponent);
