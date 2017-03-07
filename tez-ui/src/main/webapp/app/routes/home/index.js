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
  title: "All DAGs",

  queryParams: {
    dagName: REFRESH,
    dagID: REFRESH,
    submitter: REFRESH,
    status: REFRESH,
    appID: REFRESH,
    callerID: REFRESH,
    queue: REFRESH,

    rowCount: REFRESH
  },

  loaderQueryParams: {
    dagName: "dagName",
    id: "dagID",
    user: "submitter",
    status: "status",
    appID: "appID",
    callerID: "callerID",
    queueName: "queue",

    limit: "rowCount",
  },

  entityType: "dag",
  loaderNamespace: "dags",

  setupController: function (controller, model) {
    this._super(controller, model);
    Ember.run.later(this, "startCrumbBubble");
  },

  // Client side filtering to ensure that records are relevant after status correction
  filterRecords: function (records, query) {
    query = {
      name: query.dagName,
      entityID: query.id,
      submitter: query.submitter,
      status: query.status,
      appID: query.appID,
      callerID: query.callerID,
      queue: query.queueName
    };

    return records.filter(function (record) {
      for(var propName in query) {
        if(query[propName] && query[propName] !== record.get(propName)) {
          return false;
        }
      }
      return true;
    });
  },

  load: function (value, query, options) {
    var loader = this._super(value, query, options),
        that = this;
    return loader.then(function (records) {
      records = that.filterRecords(records, query);
      records.forEach(function (record) {
        if(record.get("status") === "RUNNING") {
          that.get("loader").loadNeed(record, "am", {reload: true}).catch(function () {
            if(!record.get("isDeleted")) {
              record.set("am", null);
            }
          });
        }
      });
      return records;
    });
  },

  actions: {
    willTransition: function () {
      var loader = this.get("loader");
      loader.unloadAll("dag");
      loader.unloadAll("ahs-app");
      this._super();
    },
  }
});
