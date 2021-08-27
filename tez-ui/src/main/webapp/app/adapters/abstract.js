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

import { computed, get, setProperties } from '@ember/object';
import { assert } from '@ember/debug';

import LoaderAdapter from './loader';

export default LoaderAdapter.extend({
  serverName: null, //Must be set by inheriting classes

  host: computed("serverName", function () {
    var serverName = this.serverName;
    return this.get(`hosts.${serverName}`);
  }),
  namespace: computed("serverName", function () {
    var serverName = this.serverName;
    return this.get(`env.app.namespaces.webService.${serverName}`);
  }),
  pathTypeHash: computed("serverName", function () {
    var serverName = this.serverName;
    return this.get(`env.app.paths.${serverName}`);
  }),

  ajaxOptions: function(url, method, options) {
    options = options || {};
    options.crossDomain = true;
    options.xhrFields = {
      withCredentials: true
    };
    options.targetServer = this.serverName;
    return this._super(url, method, options);
  },

  pathForType: function(type) {
    var serverName = this.serverName,
        path = this.pathTypeHash[type];
    assert(`Path not found for type:${type} to server:${serverName}`, path);
    return path;
  },

  normalizeErrorResponse: function (status, headers, payload) {
    var title;
    switch(typeof payload) {
      case "object": {
        title = payload.message;
        break;
      }
      case "string": {
        // sanitize the message
        let div = document.createElement('div');
        div.innerHTML = payload.bold();
        let scripts = div.getElementsByTagName('script')
        for(let i = 0, len = scripts.length; i < len; i++) {
          scripts[i].parentNode.removeChild(scripts[i]);
        }
        let styles = div.getElementsByTagName('style')
        for(let i = 0, len = styles.length; i < len; i++) {
          styles[i].parentNode.removeChild(styles[i]);
        }
        payload = div.textContent.trim();
        break;
      }
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
          adapterName: this.name,
          url: url
        },
        that = this;

    return this._super(url, queryParams, namespace).catch(function (error) {
      var message = `${error.message} »`,
          status = get(error, "errors.0.status");

      if(status === 0) {
        let outOfReachMessage = that.get("outOfReachMessage");
        message = `${message} ${outOfReachMessage}`;
      }
      else {
        let title = get(error, "errors.0.title") || `Error accessing ${url}`;
        message = `${message} ${status}: ${title}`;
      }

      requestInfo.responseHeaders = get(error, "errors.0.headers");
      if(queryParams) {
        requestInfo.queryParams = queryParams;
      }
      if(namespace) {
        requestInfo.namespace = namespace;
      }

      setProperties(error, {
        message: message,
        details: get(error, "errors.0.detail"),
        requestInfo: requestInfo
      });

      throw(error);
    });
  }
});
