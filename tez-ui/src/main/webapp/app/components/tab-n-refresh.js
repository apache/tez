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

import Component from '@ember/component';
import { computed } from '@ember/object';
import { inject as service } from '@ember/service';

export default Component.extend({

  router: service('router'),
  classNames: ["tab-n-refresh"],

  autoRefreshVisible: true,
  loadTime: null,
  tabs: null,

  normalizedTabs: computed("tabs", "router.currentRouteName", function () {
    var tabs = this.tabs || [],
        activeRouteName = this.router.currentRouteName;

    return tabs.map(function (tab) {
      return {
        text: tab.text,
        routeName: tab.routeName,
        active: tab.routeName === activeRouteName
      };
    });
  }),
});
