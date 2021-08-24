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
import layout from '../templates/components/em-table-pagination-ui';

export default Ember.Component.extend({
  layout: layout,

  tableDefinition: null,
  dataProcessor: null,

  classNames: ['pagination-ui'],
  isVisible: Ember.computed.alias('tableDefinition.enablePagination'),

  showFirst: Ember.computed('_possiblePages', function () {
    return this.get("dataProcessor.totalPages") && this.get('_possiblePages.0.pageNum') !== 1;
  }),

  showLast: Ember.computed('_possiblePages', 'dataProcessor.totalPages', function () {
    var possiblePages = this.get("_possiblePages");
    if(possiblePages.length) {
      return possiblePages[possiblePages.length - 1].pageNum !== this.get("dataProcessor.totalPages");
    }
  }),

  rowCountOptions: Ember.computed('tableDefinition.rowCountOptions', 'tableDefinition.rowCount', function () {
    var options = this.get('tableDefinition.rowCountOptions'),
        rowCount = this.get('tableDefinition.rowCount');

    return options.map(function (option) {
      return {
        value: option,
        selected: option === rowCount
      };
    });
  }),

  _possiblePages: Ember.computed('tableDefinition.pageNum', 'dataProcessor.totalPages', function () {
    var pageNum = this.get('tableDefinition.pageNum'),
        totalPages = this.get('dataProcessor.totalPages'),
        possiblePages = [],
        startPage = 1,
        endPage = totalPages,
        delta = 0;

    if(totalPages > 5) {
      startPage = pageNum - 2;
      endPage = pageNum + 2;

      if(startPage < 1) {
        delta = 1 - startPage;
      }
      else if(endPage > totalPages) {
        delta = totalPages - endPage;
      }

      startPage += delta;
      endPage += delta;
    }

    while(startPage <= endPage) {
      possiblePages.push({
        isCurrent: startPage === pageNum,
        pageNum: startPage++
      });
    }

    return possiblePages;
  }),

  actions: {
    rowSelected: function (value) {
      value = parseInt(value);
      if(this.get('tableDefinition.rowCount') !== value) {
        this.get('parentView').send('rowChanged', value);
      }
    },
    changePage: function (value) {
      this.get('parentView').send('pageChanged', value);
    }
  }
});
