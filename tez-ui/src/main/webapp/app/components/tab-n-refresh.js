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

export default Ember.Component.extend({
  init: function () {
    this._super();
    this.setApplication();
  },

  autoRefreshEnabled: false,
  autoRefreshVisible: true,

  setApplication: function () {
    var application = this.get("targetObject.container").lookup('controller:application');
    this.set("application", application);
  },

  autoRefreshObserver: Ember.observer("autoRefreshEnabled", function () {
    this.get('targetObject').send('autoRefreshChanged', this.get("autoRefreshEnabled"));
  }),

  normalizedTabs: Ember.computed("tabs", "application.currentPath", function () {
    var tabs = this.get("tabs") || [],
        activeRouteName = this.get("application.currentPath");

    return tabs.map(function (tab) {
      return {
        text: tab.text,
        routeName: tab.routeName,
        active: tab.routeName === activeRouteName
      };
    });
  }),

  actions: {
    refresh: function () {
      this.get('targetObject').send('reload');
    }
  }
});
