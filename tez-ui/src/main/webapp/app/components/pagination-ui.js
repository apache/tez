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
import { action, computed } from '@ember/object';
import { not } from '@ember/object/computed';

export default Component.extend({
  attributeBindings: ['shouldHide:hidden'],
  tableDefinition: null,
  dataProcessor: null,

  classNames: ['pagination-ui'],
  shouldHide: not('tableDefinition.enablePagination'),

  showFirst: computed('_possiblePages.0.pageNum', 'dataProcessor.totalPages', function () {
    return this.get("dataProcessor.totalPages") && this.get('_possiblePages.0.pageNum') !== 1;
  }),

  rowCountOptions: computed('tableDefinition.rowCountOptions', 'tableDefinition.rowCount', function () {
    var options = this.get('tableDefinition.rowCountOptions'),
        rowCount = this.get('tableDefinition.rowCount');

    return options.map(function (option) {
      return {
        value: option,
        selected: option === rowCount
      };
    });
  }),

  _possiblePages: computed('tableDefinition.pageNum',
      'tableDefinition.moreAvailable',
      'dataProcessor.totalPages', function () {
    var pageNum = this.get('tableDefinition.pageNum'),
        totalPages = this.get('dataProcessor.totalPages'),
        possiblePages = [],
        startPage = 1,
        endPage = totalPages,
        delta = 0;

    if(this.get('tableDefinition.moreAvailable')) {
      totalPages++;
    }

    if(totalPages > 1) {
      startPage = pageNum - 1;
      endPage = pageNum + 1;

      if(startPage < 1) {
        delta = 1 - startPage;
      }
      else if(endPage > totalPages) {
        delta = totalPages - endPage;
      }

      startPage += delta;
      endPage += delta;
    }

    startPage = Math.max(startPage, 1);
    endPage = Math.min(endPage, totalPages);

    while(startPage <= endPage) {
      possiblePages.push({
        isCurrent: startPage === pageNum,
        isLoadPage: startPage === totalPages,
        pageNum: startPage++,
      });
    }

    return possiblePages;
  }),

  rowSelected: action(function (value) {
    value = parseInt(value);
    if(this.get('tableDefinition.rowCount') !== value) {
      this.rowChanged(value);
    }
  }),
  changePage: action(function (value) {
    if(value === 1) {
      this.parentView.sendAction('reload');
    }
    else if(this.get('dataProcessor.totalPages') < value) {
      this.parentView.sendAction('loadPage', value);
    }
    else {
      this.parentView.send('pageChanged', value);
    }
  })
});
