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

import { action, computed, observer } from '@ember/object';
import Controller from '@ember/controller';
import { later } from '@ember/runloop';

import NameMixin from '../mixins/name';

export default Controller.extend(NameMixin, {
  // Must be set by inheriting classes
  breadcrumbs: null,

  // Must be set from abstract route
  loadTime: null,
  isMyLoading: false,

  modelObserver: observer('model', function () {
    later(this, "setBreadcrumbs");
  }),

  loaded: computed("model", "isMyLoading", function () {
    return this.model && !this.isMyLoading;
  }),

  crumbObserver: observer("breadcrumbs", function () {
    later(this, "setBreadcrumbs");
  }),

  setBreadcrumbs: function () {
    var crumbs = {},
        name = this.name;
    crumbs[name] = this.breadcrumbs;
    this.send("setBreadcrumbs", crumbs);
    this.send("bubbleBreadcrumbs", []);
  },

  refresh: action(function () {
    this.send('reload');
  })
});
