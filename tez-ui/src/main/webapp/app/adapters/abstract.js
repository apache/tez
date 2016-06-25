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

  normalizeErrorResponse: function (status, headers, payload) {
    var title;
    switch(typeof payload) {
      case "object":
        title = payload.message;
      break;
      case "string":
        let html = Ember.$(payload.bold());
        html.find('script').remove();
        html.find('style').remove();
        payload = html.text().trim();
      break;
    }

    return [{
      title: title,
      status: status,
      headers: headers,
      detail: payload
    }];
  },

  _loaderAjax: function (url, queryParams, namespace) {
    var requestInfo = {
          adapterName: this.get("name"),
          url: url
        },
        that = this;

    return this._super(url, queryParams, namespace).catch(function (error) {
      var message = `${error.message} »`,
          status = Ember.get(error, "errors.0.status");

      if(status === 0) {
        let outOfReachMessage = that.get("outOfReachMessage");
        message = `${message} ${outOfReachMessage}`;
      }
      else {
        let title = Ember.get(error, "errors.0.title") || `Error accessing ${url}`;
        message = `${message} ${status}: ${title}`;
      }

      requestInfo.responseHeaders = Ember.get(error, "errors.0.headers");
      if(queryParams) {
        requestInfo.queryParams = queryParams;
      }
      if(namespace) {
        requestInfo.namespace = namespace;
      }

      Ember.setProperties(error, {
        message: message,
        details: Ember.get(error, "errors.0.detail"),
        requestInfo: requestInfo
      });

      throw(error);
    });
  }
});
