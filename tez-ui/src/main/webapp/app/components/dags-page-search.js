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
import { action } from '@ember/object';
import { oneWay } from '@ember/object/computed';

export default Component.extend({
  classNames: ['dags-page-search'],

  dagName: oneWay("tableDefinition.dagName"),
  dagID: oneWay("tableDefinition.dagID"),
  submitter: oneWay("tableDefinition.submitter"),
  status: oneWay("tableDefinition.status"),
  appID: oneWay("tableDefinition.appID"),
  callerID: oneWay("tableDefinition.callerID"),
  queue: oneWay("tableDefinition.queue"),

  sendSearch: function () {
    this.homeSearch({
      dagName: this.dagName,
      dagID: this.dagID,
      submitter: this.submitter,
      status: this.status,
      appID: this.appID,
      callerID: this.callerID,
      queue: this.queue,
    });
  },

  statusChanged: action(function (value) {
    this.set("status", value);
  }),
  statusKeyPress: action(function () {
    this.sendSearch();
  }),
  dagSearch: action(function () {
    this.sendSearch();
  })
});
