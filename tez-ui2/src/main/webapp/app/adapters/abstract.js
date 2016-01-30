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

import LoaderAdapter from './loader';

export default LoaderAdapter.extend({
  serverName: null, //Must be set by inheriting classes

  host: Ember.computed("serverName", function () {
    var serverName = this.get("serverName");
    return this.get(`hosts.${serverName}`);
  }),
  namespace: Ember.computed("serverName", function () {
    var serverName = this.get("serverName");
    return this.get(`env.app.namespaces.webService.${serverName}`);
  }),
  pathTypeHash: Ember.computed("serverName", function () {
    var serverName = this.get("serverName");
    return this.get(`env.app.paths.${serverName}`);
  }),

  ajaxOptions: function(url, method, options) {
    options = options || {};
    options.crossDomain = true;
    options.xhrFields = {
      withCredentials: true
    };
    options.targetServer = this.get('serverName');
    return this._super(url, method, options);
  },

  pathForType: function(type) {
    var serverName = this.get("serverName"),
        path = this.get("pathTypeHash")[type];
    Ember.assert(`Path not found for type:${type} to server:${serverName}`, path);
    return path;
  },

  normalizeErrorResponse: function(status, headers, payload) {
    var response;

    if(payload && payload.exception && !payload.errors) {
      payload = `${payload.exception}\n${payload.message}\n${payload.javaClassName}`;
      response = this._super(status, headers, payload);
    }
    else {
      response = this._super(status, headers, payload);
      Ember.set(response, '0.title', this.get("outOfReachMessage"));
    }

    return response;
  }
});
