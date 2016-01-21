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

  actions: {
    dagNameChanged: function (value) {
      this.get('targetObject.targetObject').send('searchChanged', 'dagName', value);
    },
    dagIDChanged: function (value) {
      this.get('targetObject.targetObject').send('searchChanged', 'dagID', value);
    },
    submitterChanged: function (value) {
      this.get('targetObject.targetObject').send('searchChanged', 'submitter', value);
    },
    statusChanged: function (value) {
      this.get('targetObject.targetObject').send('searchChanged', 'status', value);
    },
    appIDChanged: function (value) {
      this.get('targetObject.targetObject').send('searchChanged', 'appID', value);
    },
    contextIDChanged: function (value) {
      this.get('targetObject.targetObject').send('searchChanged', 'contextID', value);
    },
  }
});
