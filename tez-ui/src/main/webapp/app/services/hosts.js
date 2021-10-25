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

import { computed } from '@ember/object';
import Service, { inject as service } from '@ember/service';

export default Service.extend({

  env: service("env"),

  correctProtocol: function (url, localProto) {
    var index = url.indexOf("://");
    if(index === -1) {
      localProto = localProto || window.location.protocol;
      return localProto + "//" + url;
    }
    var urlProto = url.substr(0, index + 1);
    if(urlProto === "file:") {
      urlProto = localProto || "http:";
      url = url.substr(index + 3);
      return urlProto + "//" + url;
    }
    return url;
  },

  normalizeURL: function (url) {
    url = this.correctProtocol(url);

    // Remove trailing slash
    if(url && url.charAt(url.length - 1) === '/') {
      url = url.slice(0, -1);
    }
    return url;
  },

  timeline: computed('env.app.hosts.timeline', function () {
    return this.normalizeURL(this.get("env.app.hosts.timeline"));
  }),

  rm: computed('env.app.hosts.rm', function () {
    return this.normalizeURL(this.get("env.app.hosts.rm"));
  }),

  am: computed('env.app.hosts.{rm,rmProxy}', function () {
    var url = this.get("env.app.hosts.rmProxy") || this.get("env.app.hosts.rm");
    return this.normalizeURL(url);
  }),

});
