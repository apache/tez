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

import Entity from './entity';

export default Entity.extend({

  idsToJoin: null,
  deferred: null,

  resetJoiner: Ember.on("init", function () {
    this.set("idsToJoin", []);
    this.set("deferred", Ember.RSVP.defer());
  }),

  queryRecord: function (loader, id, options, query, urlParams) {
    this.get("idsToJoin").push(query[this.get("queryPropertyToJoin")]);

    // Yup, only the last query would be taken by design
    Ember.run.once(this, "queryJoinedRecords", loader, options, query, urlParams);

    return this.get("deferred.promise").then(function (recordHash) {
      return recordHash[id];
    });
  },

  queryJoinedRecords: function (loader, options, query, urlParams) {
    var deferred = this.get("deferred");

    query[this.get("queryPropertyToJoin")] = this.get("idsToJoin").join(",");
    this.query(loader, query, options, urlParams).then(function (records) {
      deferred.resolve(records.reduce(function (recordHash, record) {
        recordHash[record.get("entityID")] = record;
        return recordHash;
      }, {}));
    }, function (error) {
      deferred.reject(error);
    }).finally(this.resetJoiner.bind(this));
  }

});
