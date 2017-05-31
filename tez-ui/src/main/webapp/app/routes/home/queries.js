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

import ServerSideOpsRoute from '../server-side-ops';

const REFRESH = {refreshModel: true};

export default ServerSideOpsRoute.extend({
  title: "Hive Queries",

  queryParams: {
    queryID: REFRESH,
    dagID: REFRESH,
    appID: REFRESH,
    executionMode: REFRESH,
    user: REFRESH,
    requestUser: REFRESH,
    tablesRead: REFRESH,
    tablesWritten: REFRESH,
    operationID: REFRESH,
    queue: REFRESH,

    rowCount: REFRESH
  },

  loaderQueryParams: {
    id: "queryID",
    DAG_ID: "dagID",
    APP_ID: "appID",
    executionMode: "executionMode",
    user: "user",
    requestuser: "requestUser",
    tablesRead: "tablesRead",
    tablesWritten: "tablesWritten",
    operationID: "operationID",
    queue: "queue",

    limit: "rowCount",
  },

  entityType: "hive-query",
  loaderNamespace: "queries",

  fromId: null,

  setupController: function (controller, model) {
    this._super(controller, model);
    Ember.run.later(this, "startCrumbBubble");
  },

  actions: {
    willTransition: function () {
      var loader = this.get("loader");
      loader.unloadAll("hive-query");
      this._super();
    },
  }
});
