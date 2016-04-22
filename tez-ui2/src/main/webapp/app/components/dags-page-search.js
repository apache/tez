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
  classNames: ['dags-page-search'],

  dagName: Ember.computed.oneWay("tableDefinition.dagName"),
  dagID: Ember.computed.oneWay("tableDefinition.dagID"),
  submitter: Ember.computed.oneWay("tableDefinition.submitter"),
  status: Ember.computed.oneWay("tableDefinition.status"),
  appID: Ember.computed.oneWay("tableDefinition.appID"),
  callerID: Ember.computed.oneWay("tableDefinition.callerID"),

  sendSearch: function () {
    this.get('parentView').sendAction('search', {
      dagName: this.get("dagName"),
      dagID: this.get("dagID"),
      submitter: this.get("submitter"),
      status: this.get("status"),
      appID: this.get("appID"),
      callerID: this.get("callerID"),
    });
  },

  actions: {
    statusChanged: function (value) {
      this.set("status", value);
    },
    statusKeyPress: function () {
      this.sendSearch();
    },
    search: function () {
      this.sendSearch();
    }
  }
});
