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

import AbstractRoute from './abstract';

const REFRESH = {refreshModel: true};

export default AbstractRoute.extend({
  title: "All DAGs",

  queryParams: {
    dagName: REFRESH,
    dagID: REFRESH,
    submitter: REFRESH,
    status: REFRESH,
    appID: REFRESH,
    callerID: REFRESH,

    rowCount: REFRESH
  },

  loaderQueryParams: {
    dagName: "dagName",
    dagID: "dagID",
    user: "submitter",
    status: "status",
    appID: "appID",
    callerID: "callerID",

    limit: "rowCount",
  },

  loaderNamespace: "dags",

  fromId: null,

  setupController: function (controller, model) {
    this._super(controller, model);
    Ember.run.later(this, "startCrumbBubble");
  },

  // Client side filtering to ensure that records are relevant after status correction
  filterRecords: function (records, query) {
    query = {
      name: query.dagName,
      entityID: query.dagID,
      submitter: query.submitter,
      status: query.status,
      appID: query.appID,
      callerID: query.callerID
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

  load: function (value, query/*, options*/) {
    var loader,
        that = this,
        limit = this.get("controller.rowCount") || query.limit;

    if(query.dagID) {
      that.set("loadedRecords", []);
      loader = this.get("loader").queryRecord('dag', query.dagID, {reload: true}).then(function (record) {
        return [record];
      });
    }
    else {
      query = Ember.$.extend({}, query, {
        limit: limit + 1
      });
      loader = this.get("loader").query('dag', query, {reload: true});
    }

    return loader.then(function (records) {

      if(records.get("length") > limit) {
        let lastRecord = records.popObject();
        that.set("controller.moreAvailable", true);
        that.set("fromId", lastRecord.get("entityID"));
      }
      else {
        that.set("controller.moreAvailable", false);
      }

      records = that.filterRecords(records, query);
      records.forEach(function (record) {
        if(record.get("status") === "RUNNING") {
          that.get("loader").loadNeed(record, "am", {reload: true}).catch(function () {
            record.set("am", null);
          });
        }
      });
      return records;
    });
  },

  loadNewPage: function () {
    var query = this.get("currentQuery"),
        that = this;

    query = Ember.$.extend({}, query, {
      fromId: this.get("fromId")
    });

    this.set("controller.loadingMore", true);
    return this.load(null, query).then(function (data) {
      if(that.get("controller.loadingMore")) {
        that.set("controller.loadingMore", false);
        that.get("loadedValue").pushObjects(data);
        return data;
      }
    });
  },

  actions: {
    setLoadTime: function (time) {
      this.set("controller.loadTime", time);
    },
    loadPage: function (page) {
      var that = this;
      if(this.get("controller.moreAvailable") && !this.get("controller.loadingMore")) {
        this.send("resetTooltip");
        this.loadNewPage().then(function (data) {
          if(data) {
            that.set("controller.pageNum", page);
          }
          return data;
        });
      }
    },
    reload: function () {
      this.set("controller.loadingMore", false);
      this.set("controller.pageNum", 1);
      this._super();
    },
    willTransition: function () {
      var loader = this.get("loader");
      loader.unloadAll("dag");
      loader.unloadAll("ahs-app");

      this.set("controller.pageNum", 1);

      this._super();
    },
  }
});
