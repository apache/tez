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
  classNames: ['queries-page-search'],

  queryID: oneWay("tableDefinition.queryID"),
  dagID: oneWay("tableDefinition.dagID"),
  appID: oneWay("tableDefinition.appID"),
  executionMode: oneWay("tableDefinition.executionMode"),
  user: oneWay("tableDefinition.user"),
  requestUser: oneWay("tableDefinition.requestUser"),
  tablesRead: oneWay("tableDefinition.tablesRead"),
  tablesWritten: oneWay("tableDefinition.tablesWritten"),
  operationID: oneWay("tableDefinition.operationID"),
  queue: oneWay("tableDefinition.queue"),

  sendSearch: function () {
    this.homeSearch({
      queryID: this.queryID,
      dagID: this.dagID,
      appID: this.appID,
      executionMode: this.executionMode,
      user: this.user,
      requestUser: this.requestUser,
      tablesRead: this.tablesRead,
      tablesWritten: this.tablesWritten,
      operationID: this.operationID,
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
