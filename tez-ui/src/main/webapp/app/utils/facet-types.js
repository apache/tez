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

var facetTypes = {
  VALUES: {
    componentName: "em-table-facet-panel-values",

    toClause: function (column, facetConditions) {
      var values, clauses = [];

      if(facetConditions) {
        if(Ember.get(facetConditions, "in.length")) {
          values = facetConditions.in.map(function (value) {
            value = value.replace(/'/g, "''");
            return `'${value}'`;
          });
          clauses.push(`${column.id} IN (${values})`);
        }

        if(Ember.get(facetConditions, "notIn.length")) {
          values = facetConditions.notIn.map(function (value) {
            value = value.replace(/'/g, "''");
            return `'${value}'`;
          });
          clauses.push(`${column.id} NOT IN (${values})`);
        }

        return clauses.join(" AND ");
      }
    },

    facetRows: function (column, rows) {
      var facetedDataHash = {},
          facetedDataArr = [];

      rows.forEach(function (row) {
        var value = column.getSearchValue(row);

        if(typeof value === "string") {
          if(!facetedDataHash[value]) {
            facetedDataHash[value] = {
              count: 0,
              value: value
            };
            facetedDataArr.push(facetedDataHash[value]);
          }
          facetedDataHash[value].count++;
        }

      });

      if(facetedDataArr.length) {
        facetedDataArr = facetedDataArr.sort(function (a, b) {
          return -(a.count - b.count); // Sort in reverse order
        });
        return facetedDataArr;
      }
    },

    normaliseConditions: function (conditions, data) {
      if(Ember.get(conditions, "in.length") < data.length) {
        return conditions;
      }
    }
  },
};

export default facetTypes;
