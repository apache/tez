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

import DS from 'ember-data';
import NameMixin from '../mixins/name';

var MoreString = more.String;

export default DS.RESTAdapter.extend(NameMixin, {
  _isLoader: true,

  buildURL: function(modelName, id, snapshot, requestType, query, params) {
    var url = this._super(modelName, id, snapshot, requestType, query);
    return params ? MoreString.fmt(url, params) : url;
  },

  _loaderAjax: function (url, queryParams, nameSpace) {
    if (this.sortQueryParams && queryParams) {
      queryParams = this.sortQueryParams(queryParams);
    }

    // Inject nameSpace
    return this.ajax(url, 'GET', { data: queryParams }).then(function (data) {
      return {
        nameSpace: nameSpace,
        data: data
      };
    });
  },

  queryRecord: function(store, type, query) {
    var queryParams = query.params,
        url = this.buildURL(type.modelName, query.id, null, null, queryParams, query.urlParams);
    return this._loaderAjax(url, queryParams, query.nameSpace);
  },

  query: function (store, type, query/*, recordArray*/) {
    var queryParams = query.params,
        url = this.buildURL(type.modelName, null, null, 'query', queryParams, query.urlParams);
    return this._loaderAjax(url, queryParams, query.nameSpace);
  }

});
