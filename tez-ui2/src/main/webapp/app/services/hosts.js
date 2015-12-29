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
import environment from '../config/environment';

export default Ember.Service.extend({

  correctProtocol: function (url) {
    var proto = window.location.protocol;

    if(proto === "file:") {
      proto = "http:";
    }

    if(url.match("://")) {
      url = url.substr(url.indexOf("://") + 3);
    }

    return proto + "//" + url;
  },

  normalizeURL: function (url) {
    url = this.correctProtocol(url);

    // Remove trailing slash
    if(url && url.charAt(url.length - 1) === '/') {
      url = url.slice(0, -1);
    }
    return url;
  },

  timelineBaseURL: Ember.computed(function () {
    // Todo: Use global ENV if available
    // Todo: Use loaded host if protocol is != file
    return this.normalizeURL(environment.hosts.timeline);
  }),

  rmBaseURL: Ember.computed(function () {
    return this.normalizeURL(environment.hosts.RM);
  }),

});
