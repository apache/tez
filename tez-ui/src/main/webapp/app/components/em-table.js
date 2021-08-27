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

import Component from '@ember/component';
import EmberObject, { action, computed, observer } from '@ember/object';
import { on } from '@ember/object/evented';
import { pluralize } from 'ember-inflector';
import Definition from '../utils/table-definition';
import ColumnDefinition from '../utils/column-definition';
import DataProcessor from '../utils/data-processor';

import layout from '../templates/components/em-table';

function createAssigner(targetPath, targetKey, sourcePath) {
  return on("init", observer(targetPath, sourcePath, function () {
    var target = this.get(targetPath),
        source = this.get(sourcePath);
    if(target && source !== undefined) {
      target.set(targetKey, source);
    }
  }));
}

export default Component.extend({
  layout: layout,

  classNames: ["em-table"],
  classNameBindings: ["showScrollShadow", "showLeftScrollShadow", "showRightScrollShadow"],

  definition: null,
  dataProcessor: null,

  headerComponentNames: computed( {
    get() {
      if (this._headerComponentNames) {
        return this._headerComponentNames;
      }
      return ['em-table-search-ui', 'em-table-pagination-ui'];
    },
    set(key, value) {
      return this._headerComponentNames = value;
    }
  }),
  footerComponentNames: computed( {
    get() {
      if (this._footerComponentNames) {
        return this._footerComponentNames;
      }
      return ['em-table-pagination-ui'];
    },
    set(key, value) {
      return this._footerComponentNames = value;
    }
  }),

  leftPanelComponentName: "em-table-facet-panel",
  rightPanelComponentName: "",

  //columnWidthChangeAction: null,

  init: function() {
    this._super(...arguments);
    this.set("scrollValues", EmberObject.create({
      left: 0,
      width: 0,
      viewPortWidth: 0
    }));
  },

  showScrollShadow: false,
  showLeftScrollShadow: false,
  showRightScrollShadow: false,

  assignDefinitionInProcessor: createAssigner('_dataProcessor', 'tableDefinition', '_definition'),
  assignRowsInProcessor: createAssigner('_dataProcessor', 'rows', 'rows'),
  assignColumnsInDefinition: createAssigner('_definition', 'columns', 'columns'),

  assignEnableSortInDefinition: createAssigner('_definition', 'enableSort', 'enableSort'),
  assignEnableSearchInDefinition: createAssigner('_definition', 'enableSearch', 'enableSearch'),
  assignEnablePaginationInDefinition: createAssigner('_definition', 'enablePagination', 'enablePagination'),
  assignRowCountInDefinition: createAssigner('_definition', 'rowCount', 'rowCount'),

  _definition: computed('definition', 'definitionClass', function () {
    return this.definition || (this.definitionClass || Definition).create();
  }),
  _dataProcessor: computed('dataProcessor', 'dataProcessorClass', function () {
    return this.dataProcessor || (this.dataProcessorClass || DataProcessor).create();
  }),

  displayFooter: computed("_definition.minRowsForFooter", "_dataProcessor.processedRows.length", function () {
    return this.get("_definition.minRowsForFooter") <= this.get("_dataProcessor.processedRows.length");
  }),

  _processedRowsObserver: observer('_dataProcessor.processedRows', function () {
    // TODO Counters have issues
    if (this.rowsChanged) {
      this.rowsChanged(this.get('_dataProcessor.processedRows'));
    }
  }),

  _setColumnWidth: function (columns) {
    var widthText = (100 / columns.length) + "%";
    columns.forEach(function (column) {
      if(!column.width) {
        column.width = widthText;
      }
    });
  },

  _columns: computed('_definition.columns', function () {
    var rawColumns = this.get('_definition.columns'),
        normalisedColumns = {
          left: [],
          center: [],
          right: [],
          length: rawColumns.length
        };

    rawColumns.forEach(function (column) {
      normalisedColumns[column.get("pin")].push({
        definition: column,
        width: column.width
      });
    });

    if(normalisedColumns.center.length === 0) {
      normalisedColumns.center = [{
        definition: ColumnDefinition.fillerColumn,
      }];
    }

    this._setColumnWidth(normalisedColumns.center);

    return normalisedColumns;
  }),

  message: computed('_columns.length', '_dataProcessor.message', '_dataProcessor.processedRows.length', '_definition.recordType', function () {
    var message = this.get("_dataProcessor.message");
    if(message) {
      return message;
    }
    else if(!this.get('_columns.length')) {
      return "No columns available!";
    }
    else if(!this.get("_dataProcessor.processedRows.length")) {
      let identifiers = pluralize(this.get('_definition.recordType') || "record");
      return `No ${identifiers} available!`;
    }
  }),

  search: action(function (searchText, actualSearchType) {
    this.set('_definition.searchText', searchText);
    this.set('_definition._actualSearchType', actualSearchType);
  }),
  sort: action(function (sortColumnId, sortOrder) {
    this._definition.setProperties({
      sortColumnId,
      sortOrder
    });
  }),
  rowChanged: action(function (rowCount) {
    this.set('_definition.rowCount', rowCount);
    this.rowCountChanged(rowCount);
  }),
  pageChanged: action(function (pageNum) {
    this.set('_definition.pageNum', pageNum);
  }),
});
