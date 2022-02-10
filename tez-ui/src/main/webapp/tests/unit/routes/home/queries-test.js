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

import { A } from '@ember/array';
import EmberObject from '@ember/object';
import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { resolve } from 'rsvp';

module('Unit | Route | home/queries', function(hooks) {
  setupTest(hooks);

  test('it exists', function(assert) {
    let route = this.owner.lookup('route:home/queries');

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
    let route = this.owner.lookup('route:home/queries');

    assert.true(route.get("queryParams.queryID.refreshModel"));
    assert.true(route.get("queryParams.dagID.refreshModel"));
    assert.true(route.get("queryParams.appID.refreshModel"));
    assert.true(route.get("queryParams.executionMode.refreshModel"));
    assert.true(route.get("queryParams.user.refreshModel"));
    assert.true(route.get("queryParams.requestUser.refreshModel"));
    assert.true(route.get("queryParams.tablesRead.refreshModel"));
    assert.true(route.get("queryParams.tablesWritten.refreshModel"));
    assert.true(route.get("queryParams.operationID.refreshModel"));
    assert.true(route.get("queryParams.queue.refreshModel"));

    assert.true(route.get("queryParams.rowCount.refreshModel"));
  });

  test('loaderQueryParams test', function(assert) {
    let route = this.owner.lookup('route:home/queries');
    assert.equal(Object.keys(route.loaderQueryParams).length, 10 + 1);
  });

  test('load - query test', function(assert) {
    let route = this.owner.factoryFor('route:home/queries').create({
          controller: EmberObject.create()
        }),
        testEntityID1 = "entity_1",
        testSubmitter = "sub",
        query = {
          limit: 5,
          submitter: testSubmitter
        },
        resultRecords = A([
          EmberObject.create({
            submitter: testSubmitter,
            entityID: testEntityID1
          })
        ]);

    route.loader = EmberObject.create({
      query: function (type, query, options) {
        assert.equal(type, "hive-query");
        assert.equal(query.limit, 6);
        assert.true(options.reload);
        return resolve(resultRecords);
      },
    });

    assert.expect(3 + 1 + 2);

    return route.load(null, query).then(function (records) {
      assert.ok(Array.isArray(records));

      assert.equal(records.get("length"), 1);
      assert.equal(records.get("0.entityID"), testEntityID1);
    });

  });

  test('actions.willTransition test', function(assert) {
    let route = this.owner.factoryFor('route:home/queries').create({
      controller: EmberObject.create()
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
});
