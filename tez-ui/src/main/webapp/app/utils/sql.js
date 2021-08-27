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

import alasql from 'alasql';
import EmberObject from '@ember/object';

/*
 * A wrapper around AlaSQL
 */
export default EmberObject.extend({

  constructQuery: function(clause) {
    return `SELECT * FROM ? WHERE ${clause}`;
  },

  validateClause: function (clause, columns) {
    clause = clause.toString();

    var query = this.constructQuery(this.normaliseClause(clause, columns || [])),
        valid = false;

    if(clause.match(/\W/g)) { // If it contain special characters including space
      try {
        alasql(query, [[{}]]);
        valid = true;
      }
      catch(e) {}
    }

    return valid;
  },

  createFacetClause: function (conditions, columns) {
    if(conditions && columns) {
      return columns.map(function (column) {
        if(column.get("facetType")) {
          return column.get("facetType.toClause")(column, conditions[column.id]);
        }
      }).filter(clause => clause).join(" AND ");
    }
  },

  normaliseClause: function (clause, columns) {
    clause = clause.toString();
    columns.forEach(function (column) {
      var headerTitle = column.get("headerTitle");
      clause = clause.replace(new RegExp(`"${headerTitle}"`, "gi"), column.get("id"));
    });
    return clause;
  },

  search: function (clause, rows, columns) {
    clause = this.normaliseClause(clause, columns);

    // Convert into a form that alasql can digest easily
    var dataSet = rows.map(function (row, index) {
      var rowObj = {
        _index_: index
      };

      columns.forEach(function (column) {
        if(column.get("enableSearch") && row) {
          rowObj[column.get("id")] = column.getSearchValue(row);
        }
      });

      return rowObj;
    });

    // Search
    dataSet = alasql(this.constructQuery(clause), [dataSet]);

    return dataSet.map(function (data) {
      return rows[data._index_];
    });
  }

});
