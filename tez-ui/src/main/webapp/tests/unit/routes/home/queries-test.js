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
import { moduleFor, test } from 'ember-qunit';

moduleFor('route:home/queries', 'Unit | Route | home/queries', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('it exists', function(assert) {
  let route = this.subject();

  assert.ok(route);
  assert.ok(route.title);

  assert.ok(route.queryParams);
  assert.ok(route.loaderQueryParams);
  assert.ok(route.setupController);

  assert.equal(route.entityType, "hive-query");
  assert.equal(route.loaderNamespace, "queries");

  assert.ok(route.actions.willTransition);
});

test('refresh test', function(assert) {
  let route = this.subject();

  assert.equal(route.get("queryParams.queryName.refreshModel"), true);
  assert.equal(route.get("queryParams.queryID.refreshModel"), true);
  assert.equal(route.get("queryParams.dagID.refreshModel"), true);
  assert.equal(route.get("queryParams.appID.refreshModel"), true);
  assert.equal(route.get("queryParams.executionMode.refreshModel"), true);
  assert.equal(route.get("queryParams.user.refreshModel"), true);
  assert.equal(route.get("queryParams.requestUser.refreshModel"), true);
  assert.equal(route.get("queryParams.tablesRead.refreshModel"), true);
  assert.equal(route.get("queryParams.tablesWritten.refreshModel"), true);
  assert.equal(route.get("queryParams.operationID.refreshModel"), true);
  assert.equal(route.get("queryParams.queue.refreshModel"), true);

  assert.equal(route.get("queryParams.rowCount.refreshModel"), true);
});

test('loaderQueryParams test', function(assert) {
  let route = this.subject();
  assert.equal(Object.keys(route.get("loaderQueryParams")).length, 11 + 1);
});

test('actions.willTransition test', function(assert) {
  let route = this.subject({
    controller: Ember.Object.create()
  });

  route.set("loader", {
    unloadAll: function (type) {
      if(type === "hive-query") {
        assert.ok(true);
      }
      else {
        throw(new Error("Invalid type!"));
      }
    }
  });

  assert.expect(1);
  route.send("willTransition");
});
