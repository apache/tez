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

export default EmberObject.extend({

  recordType: "",

  // Search
  enableSearch: true,
  searchText: '',
  searchType: 'auto', // Can be either of auto, manual, regex OR sql
  _actualSearchType: "Regex", // Set from em-table-search-ui

  // Faceting
  enableFaceting: false,
  facetConditions: null,
  minFieldsForFilter: 15,
  minValuesToDisplay: 2,
  facetValuesPageSize: 10,

  // Sort
  enableSort: true,
  sortColumnId: '',
  sortOrder: '',
  headerAsSortButton: false,

  // Pagination
  enablePagination: true,
  pageNum: 1,
  rowCount: 10,
  rowCountOptions: computed(function() { return [5, 10, 25, 50, 100] }),

  enableColumnResize: true,
  showScrollShadow: false,

  minRowsForFooter: 25,

  columns: [],

  _pageNumResetObserver: observer('searchText', 'facetConditions', 'rowCount', function () {
    this.set('pageNum', 1);
  }),

});
