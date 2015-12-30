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

import Ember from 'ember';
import DS from 'ember-data';

var MoreString = more.String;

export default DS.RESTAdapter.extend({
  ajax: function(url, method, hash) {
    return this._super(url, method, Ember.$.extend(hash || {}, {
      crossDomain: true,
      xhrFields: {
        withCredentials: true
      }
    }));
  },
  buildURL: function(type, id, record) {
    var url = this._super(type, undefined, record);
    return MoreString.fmt(url, record);
  },
  findQuery: function(store, type, query) {
    var record = query.metadata;
    delete query.metadata;

    return this.ajax(this.buildURL(
        Ember.String.pluralize(type.typeKey),
        record.id,
        Ember.Object.create(record)
      ),
      'GET',
      {
        data: query
      }
    );
  }
});
