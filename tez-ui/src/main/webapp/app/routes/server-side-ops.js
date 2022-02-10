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

import { action } from '@ember/object';
import { assign } from '@ember/polyfills';
import AbstractRoute from './abstract';

export default AbstractRoute.extend({

  entityType: '',
  fromId: null,

  load: function (value, query/*, options*/) {
    var loader,
        that = this,
        limit = this.get("controller.rowCount") || query.limit,
        entityType = this.entityType;

    if(query.id) {
      that.set("loadedRecords", []);
      loader = this.loader.queryRecord(entityType, query.id, {
        reload: true
      }).then(function (record) {
        return [record];
      },function () {
        return [];
      });
    }
    else {
      query = assign({}, query, {
        limit: limit + 1
      });
      loader = this.loader.query(entityType, query, {reload: true});
    }

    return loader.then(function (records) {
      if(records.get("length") > limit) {
        let lastRecord = records.popObject();
        that.set("controller.moreAvailable", true);
        that.set("fromId", lastRecord.get("entityID"));
      }
      else {
        that.set("controller.moreAvailable", false);
        that.set("fromId", null);
      }
      return records;
    });
  },

  loadNewPage: function () {
    var query = this.currentQuery,
        that = this;

    query = assign({}, query, {
      fromId: this.fromId
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

  loadPage: action(function (page) {
    var that = this;
    if(this.get("controller.moreAvailable") && !this.get("controller.loadingMore")) {
      this.loadNewPage().then(function (data) {
        if(data) {
          that.set("controller.pageNum", page);
        }
        return data;
      });
    }
  }),
  reload: action(function () {
    this.set("controller.loadingMore", false);
    this.set("controller.pageNum", 1);
    this.reloadAction();
  }),

  willTransition: action(function () {
    this.set("controller.pageNum", 1);
    this._super();
  })
});
