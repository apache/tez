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
import layout from '../templates/components/em-table-facet-panel-values';

const LIST_LIMIT = 7;

export default Ember.Component.extend({
  layout: layout,

  data: null,
  checkedCount: null,

  tableDefinition: null,
  dataProcessor: null,

  tmpFacetConditions: null,

  hideValues: true,

  currentPage: 1,

  classNames: ['em-table-facet-panel-values'],
  classNameBindings: ['hideValues', 'hideFilter', 'hideSelectAll'],

  filterText: null,
  allButtonTitle: Ember.computed("filterText", function () {
    let filterText = this.get("filterText");
    return filterText ? `Select all with substring '${filterText}'` : "Select all";
  }),
  isVisible: Ember.computed("data.facets.length", "tableDefinition.minValuesToDisplay", function () {
    return this.get("data.facets.length") >= this.get("tableDefinition.minValuesToDisplay");
  }),
  hideFilter: Ember.computed("allFacets.length", function () {
    return this.get("allFacets.length") < LIST_LIMIT;
  }),
  hideSelectAll: Ember.computed("fieldFacetConditions", "checkedCount", "data.facets", function () {
    return this.get("fieldFacetConditions.in.length") === this.get("data.facets.length");
  }),

  fieldFacetConditions: Ember.computed("tmpFacetConditions", "data.column.id", function () {
    var columnID = this.get("data.column.id"),
        conditions = this.get(`tmpFacetConditions.${columnID}`),
        facets = this.get("data.facets") || [];

    if(!conditions) {
      conditions = {
        in: facets.map(facet => facet.value)
      };
      this.set(`tmpFacetConditions.${columnID}`, conditions);
    }

    return conditions;
  }),

  allFacets: Ember.computed("data.facets", "fieldFacetConditions", function () {
    var facets = this.get("data.facets") || [],

        checkedValues = this.get("fieldFacetConditions.in"),
        selectionHash = {};

    if(checkedValues) {
      checkedValues.forEach(function (valueText) {
        selectionHash[valueText] = 1;
      });
    }

    return Ember.A(facets.map(function (facet) {
      facet = Ember.Object.create(facet);
      facet.set("checked", selectionHash[facet.value]);

      if(!facet.get("displayText")) {
        facet.set("displayText", facet.get("value"));
      }

      return facet;
    }));
  }),

  filteredFacets: Ember.computed("allFacets", "filterText", function () {
    var allFacets = this.get("allFacets"),
        filterText = this.get("filterText"),
        filteredFacets;

    if(filterText) {
      filteredFacets = allFacets.filter(function (facet) {
        return facet.get("value").match(filterText);
      });
    }
    else {
      filteredFacets = allFacets;
    }

    return filteredFacets;
  }),

  _filterObserver: Ember.observer("filterText", function () {
    this.set("currentPage", 1);
  }),

  totalPages: Ember.computed("filteredFacets.length", "tableDefinition.facetValuesPageSize", function () {
    return Math.ceil(this.get("filteredFacets.length") / this.get("tableDefinition.facetValuesPageSize"));
  }),
  showPagination: Ember.computed("totalPages", function () {
    return this.get("totalPages") > 1;
  }),
  showPrevious: Ember.computed("currentPage", function () {
    return this.get("currentPage") > 1;
  }),
  showNext: Ember.computed("currentPage", "totalPages", function () {
    return this.get("currentPage") < this.get("totalPages");
  }),

  paginatedFacets: Ember.computed("filteredFacets", "currentPage", "tableDefinition.facetValuesPageSize", function () {
    let currentPage = this.get("currentPage"),
        pageSize = this.get("tableDefinition.facetValuesPageSize");
    return this.get("filteredFacets").slice(
      (currentPage - 1) * pageSize,
      currentPage * pageSize);
  }),

  actions: {
    changePage: function (factor) {
      var newPage = this.get("currentPage") + factor;
      if(newPage > 0 && newPage <= this.get("totalPages")) {
        this.set("currentPage", newPage);
      }
    },
    toggleValueDisplay: function () {
      this.toggleProperty("hideValues");
      this.get("parentView").sendAction("toggleValuesDisplayAction", !this.get("hideValues"), this.get("data"));
    },
    clickedCheckbox: function (facet) {
      var checkedValues = this.get("fieldFacetConditions.in"),
          value = facet.get("value"),
          valueIndex = checkedValues.indexOf(value);

      facet.toggleProperty("checked");

      if(facet.get("checked")) {
        if(valueIndex === -1) {
          checkedValues.push(value);
        }
      }
      else if(valueIndex !== -1) {
        checkedValues.splice(valueIndex, 1);
      }

      this.set("checkedCount", checkedValues.length);
    },

    selectAll: function () {
      var filteredFacets = this.get("filteredFacets"),
          checkedValues = this.get("fieldFacetConditions.in");

      filteredFacets.forEach(function (facet) {
        if(!facet.get("checked")) {
          checkedValues.push(facet.get("value"));
        }

        facet.set("checked", true);
      });

      this.set("fieldFacetConditions.in", checkedValues);
      this.set("checkedCount", checkedValues.length);
    },
    clickedOnly: function (facet) {
      var allFacets = this.get("allFacets"),
          checkedValues = [];

      allFacets.forEach(function (facet) {
        facet.set("checked", false);
      });

      facet.set("checked", true);
      checkedValues.push(facet.get("value"));

      this.set("fieldFacetConditions.in", checkedValues);
      this.set("checkedCount", checkedValues.length);
    }
  }

});
