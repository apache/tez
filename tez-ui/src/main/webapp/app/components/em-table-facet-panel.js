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
import layout from '../templates/components/em-table-facet-panel';

export default Ember.Component.extend({
  layout: layout,

  classNames: ["em-table-facet-panel"],
  classNameBindings: ['isEmpty', 'hideFilter'],

  isVisible: Ember.computed.alias('tableDefinition.enableFaceting'),

  tableDefinition: null,
  dataProcessor: null,
  tmpFacetConditions: {},

  filterText: null,
  isEmpty: Ember.computed("dataProcessor.facetedFields.length", function () {
    return this.get("dataProcessor.facetedFields.length") === 0;
  }),
  hideFilter: Ember.computed("dataProcessor.facetedFields.length", "tableDefinition.minFieldsForFilter", function () {
    return this.get("dataProcessor.facetedFields.length") < this.get("tableDefinition.minFieldsForFilter");
  }),

  didInsertElement: Ember.observer("filterText", "dataProcessor.facetedFields", function () {
    var fields = this.get("dataProcessor.facetedFields"),
        filterText = this.get("filterText"),
        filterRegex = new RegExp(filterText, "i"),
        elements = Ember.$(this.get("element")).find(".field-list>li");

    elements.each(function (index, element) {
      var foundMatch = !filterText || Ember.get(fields, `${index}.column.headerTitle`).match(filterRegex);
      Ember.$(element)[foundMatch ? "show" : "hide"]();
    });
  }),

  _facetConditionsObserver: Ember.observer("tableDefinition.facetConditions", "dataProcessor.processedRows.[]", function () {
    var facetConditions = Ember.$.extend({}, this.get("tableDefinition.facetConditions"));
    this.set("tmpFacetConditions", facetConditions);
  }),

  actions: {
    applyFilters: function () {
      var tmpFacetConditions = this.get("tmpFacetConditions"),
          facetedFields = this.get("dataProcessor.facetedFields"),
          normalizedTmpFacetConditions = {};

      facetedFields.forEach(function (field) {
        var column = field.column,
            columnId = column.get("id"),
            facetType = column.get("facetType"),
            normalizedConditions;

        if(facetType) {
          normalizedConditions = facetType.normaliseConditions(tmpFacetConditions[columnId], field.facets);
          if(normalizedConditions) {
            normalizedTmpFacetConditions[columnId] = normalizedConditions;
          }
        }
      });

      this.set("tableDefinition.facetConditions", normalizedTmpFacetConditions);
    },
    clearFilters: function () {
      this.set("tmpFacetConditions", {});
      this.set("tableDefinition.facetConditions", {});
    },
  }
});
