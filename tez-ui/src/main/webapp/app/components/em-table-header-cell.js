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
import layout from '../templates/components/em-table-header-cell';

export default Component.extend({
  layout: layout,

  title: null, // Header cell Name
  attributeBindings: ['definition.id:title'],

  definition: null,
  tableDefinition: null,
  dataProcessor: null,

  classNames: ['table-header-cell'],
  classNameBindings: ['isSorting'],

  isSorting: computed('dataProcessor.isSorting', 'definition.id', 'tableDefinition.sortColumnId', function () {
    return this.get("dataProcessor.isSorting") && this.get('tableDefinition.sortColumnId') === this.get('definition.id');
  }),

  sortIconCSS: computed('definition.id', 'tableDefinition.{sortColumnId,sortOrder}', function () {
    if(this.get('tableDefinition.sortColumnId') === this.get('definition.id')) {
      return this.get('tableDefinition.sortOrder');
    }
  }),

  sortToggledTitle: computed('definition.id', 'tableDefinition.{sortColumnId,sortOrder}', function () {
    if(this.get('tableDefinition.sortColumnId') === this.get('definition.id')) {
      switch(this.get('tableDefinition.sortOrder')) {
        case "asc":
          return "descending";
        case "desc":
          return "ascending";
      }
    }
  }),

  sort: action(function () {
    this.parentView.send('sort');
  }),
  startColResize: action(function () {
    this.parentView.send('startColResize');
  })
});
