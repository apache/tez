/*global more*/

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

import AbstractAdapter from './abstract';

var MoreObject = more.Object;

export default AbstractAdapter.extend({
  serverName: "timeline",
  outOfReachMessage: "Timeline server (ATS) is out of reach. Either it's down, or CORS is not enabled.",

  filters: {
    dagID: 'TEZ_DAG_ID',
    vertexID: 'TEZ_VERTEX_ID',
    taskID: 'TEZ_TASK_ID',
    attemptID: 'TEZ_TASK_ATTEMPT_ID',
    hiveQueryID: 'HIVE_QUERY_ID',
    appID: 'applicationId',

    dagName: 'dagName',
    user: "user",
    status: "status",
    callerID: "callerId"
  },

  stringifyFilters: function (filters) {
    var filterStrs = [];

    MoreObject.forEach(filters, function (key, value) {
      value = JSON.stringify(String(value));
      filterStrs.push(`${key}:${value}`);
    });

    return filterStrs.join(",");
  },

  normalizeQuery: function(query) {
    var primaryFilter = null, // Primary must have just one single filter
        secondaryFilters = {},
        normalQuery = {},
        filterStr;

    MoreObject.forEach(query, function (key, value) {
      var filter = this.get(`filters.${key}`);

      if(filter) {
        if(!primaryFilter) {
          primaryFilter = {};
          primaryFilter[filter] = value;
        }
        else {
          secondaryFilters[filter] = value;
        }
      }
      else {
        normalQuery[key] = value;
      }
    }, this);

    // primaryFilter
    if(primaryFilter) {
      filterStr = this.stringifyFilters(primaryFilter);
    }
    if(filterStr) {
      normalQuery.primaryFilter = filterStr;
    }

    // secondaryFilters
    filterStr = this.stringifyFilters(secondaryFilters);
    if(filterStr) {
      normalQuery.secondaryFilter = filterStr;
    }

    // Limit
    normalQuery.limit = normalQuery.limit || this.get("env.app.rowLoadLimit");

    return normalQuery;
  },

  query: function (store, type, query/*, recordArray*/) {
    var queryParams = query.params,
        url = this.buildURL(type.modelName, null, null, 'query', queryParams, query.urlParams);

    if(query) {
      queryParams = this.normalizeQuery(queryParams);
    }

    return this._loaderAjax(url, queryParams, query.nameSpace);
  }
});
