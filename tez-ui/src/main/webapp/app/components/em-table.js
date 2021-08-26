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
import Definition from '../utils/table-definition';
import ColumnDefinition from '../utils/column-definition';
import DataProcessor from '../utils/data-processor';

import layout from '../templates/components/em-table';

const DEFAULT_ROW_HIGHLIGHT_COLOR = "#EEE";

function createAssigner(targetPath, targetKey, sourcePath) {
  return Ember.on("init", Ember.observer(targetPath, sourcePath, function () {
    var target = this.get(targetPath),
        source = this.get(sourcePath);
    if(target && source !== undefined) {
      target.set(targetKey, source);
    }
  }));
}

const HANDLERS = {
  // Mouse handlers
  mouseOver: function(event) {
    var index = Ember.$(this).index() + 1;
    event.data.highlightRow(index);
  },
  mouseLeave: function(event) {
    event.data.highlightRow(-1);
  },

  // Scroll handler
  onScroll: function(event) {
    var tableBody = event.currentTarget,
        scrollValues = event.data.get("scrollValues");

    scrollValues.set("left", tableBody.scrollLeft);
    scrollValues.set("width", tableBody.scrollWidth);
  }
};

export default Ember.Component.extend({
  layout: layout,

  classNames: ["em-table"],
  classNameBindings: ["showScrollShadow", "showLeftScrollShadow", "showRightScrollShadow"],

  definition: null,
  dataProcessor: null,

  highlightRowOnMouse: false, // Could be true or {color: "#XYZ"}

  headerComponentNames: ['em-table-search-ui', 'em-table-pagination-ui'],
  footerComponentNames: ['em-table-pagination-ui'],

  leftPanelComponentName: "em-table-facet-panel",
  rightPanelComponentName: "",

  columnWidthChangeAction: null,

  scrollChangeAction: null,
  scrollValues: null,
  _widthTrackerTimer: null,

  init: function() {
    this._super();
    this.set("scrollValues", Ember.Object.create({
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

  _definition: Ember.computed('definition', 'definitionClass', function () {
    return this.get('definition') || (this.get('definitionClass') || Definition).create();
  }),
  _dataProcessor: Ember.computed('dataProcessor', 'dataProcessorClass', function () {
    return this.get('dataProcessor') || (this.get('dataProcessorClass') || DataProcessor).create();
  }),

  displayFooter: Ember.computed("_definition.minRowsForFooter", "_dataProcessor.processedRows.length", function () {
    return this.get("_definition.minRowsForFooter") <= this.get("_dataProcessor.processedRows.length");
  }),

  _processedRowsObserver: Ember.observer('_dataProcessor.processedRows', function () {
    this.sendAction('rowsChanged', this.get('_dataProcessor.processedRows'));
  }),

  _setColumnWidth: function (columns) {
    var widthText = (100 / columns.length) + "%";
    columns.forEach(function (column) {
      if(!column.width) {
        column.width = widthText;
      }
    });
  },

  _columns: Ember.computed('_definition.columns', function () {
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

  message: Ember.computed('_dataProcessor.message', '_columns.length', '_dataProcessor.processedRows.length', function () {
    var message = this.get("_dataProcessor.message");
    if(message) {
      return message;
    }
    else if(!this.get('_columns.length')) {
      return "No columns available!";
    }
    else if(!this.get("_dataProcessor.processedRows.length")) {
      let identifiers = Ember.String.pluralize(this.get('_definition.recordType') || "record");
      return `No ${identifiers} available!`;
    }
  }),

  highlightRow: function (index) {
    var element = Ember.$(this.get("element")),
        sheet = element.find("style")[0].sheet,
        elementID = element.attr("id"),
        color = this.get("highlightRowOnMouse.color") || DEFAULT_ROW_HIGHLIGHT_COLOR;

    try {
      sheet.deleteRule(0);
    }catch(e){}

    if(index >= 0) {
      sheet.insertRule(`#${elementID} .table-cell:nth-child(${index}){ background-color: ${color}; }`, 0);
    }
  },

  didInsertElement: function () {
    Ember.run.scheduleOnce('afterRender', this, function() {
      this.highlightRowOnMouseObserver();
      this.scrollChangeActionObserver();
    });
  },

  highlightRowOnMouseObserver: Ember.observer("highlightRowOnMouse", function () {
    var highlightRowOnMouse = this.get("highlightRowOnMouse"),
        element = this.get("element");

    if(element) {
      element = Ember.$(element).find(".table-mid");

      if(highlightRowOnMouse) {
        element.on('mouseover', '.table-cell', this, HANDLERS.mouseOver);
        element.on('mouseleave', this, HANDLERS.mouseLeave);
      }
      else {
        element.off('mouseover', '.table-cell', HANDLERS.mouseOver);
        element.off('mouseleave', HANDLERS.mouseLeave);
      }
    }
  }),

  scrollValuesObserver: Ember.observer("scrollValues.left", "scrollValues.width", "scrollValues.viewPortWidth", function () {
    var scrollValues = this.get("scrollValues");

    this.sendAction("scrollChangeAction", scrollValues);


    this.set("showLeftScrollShadow", scrollValues.left > 1);
    this.set("showRightScrollShadow", scrollValues.left < (scrollValues.width - scrollValues.viewPortWidth));
  }),

  scrollChangeActionObserver: Ember.observer("scrollChangeAction", "message", "showScrollShadow", function () {
    Ember.run.scheduleOnce('afterRender', this, function() {
      var addScrollListener = this.get("scrollChangeAction") || this.get("showScrollShadow"),
          element = this.$().find(".table-body"),
          scrollValues = this.get("scrollValues");

      if(addScrollListener && element) {
        element = element.get(0);

        clearInterval(this.get("_widthTrackerTimer"));

        if(element) {
          if(addScrollListener) {
            Ember.$(element).on('scroll', this, HANDLERS.onScroll);

            this.set("_widthTrackerTimer", setInterval(function () {
              scrollValues.setProperties({
                width: element.scrollWidth,
                viewPortWidth: element.offsetWidth
              });
            }, 1000));
          }
          else {
            element.off('scroll', HANDLERS.onScroll);
          }
        }
      }
    });
  }),

  willDestroyElement: function () {
    this._super();
    clearInterval(this.get("_widthTrackerTimer"));
    Ember.$(this.$().find(".table-body")).off();
    Ember.$(this.$().find(".table-mid")).off();
    Ember.$(this.$()).off();
  },

  actions: {
    search: function (searchText, actualSearchType) {
      this.set('_definition.searchText', searchText);
      this.set('_definition._actualSearchType', actualSearchType);
      this.sendAction("searchAction", searchText);
    },
    sort: function (sortColumnId, sortOrder) {
      this.get("_definition").setProperties({
        sortColumnId,
        sortOrder
      });
      this.sendAction("sortAction", sortColumnId, sortOrder);
    },
    rowChanged: function (rowCount) {
      this.set('_definition.rowCount', rowCount);
      this.sendAction("rowAction", rowCount);
    },
    pageChanged: function (pageNum) {
      this.set('_definition.pageNum', pageNum);
      this.sendAction("pageAction", pageNum);
    },
    columnWidthChanged: function (width, columnDefinition, index) {
      this.sendAction("columnWidthChangeAction", width, columnDefinition, index);
    }
  }
});
