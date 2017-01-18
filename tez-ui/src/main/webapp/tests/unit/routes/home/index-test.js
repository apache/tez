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

moduleFor('route:home/index', 'Unit | Route | home/index', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let route = this.subject();

  assert.ok(route);
  assert.ok(route.title);

  assert.ok(route.queryParams);
  assert.ok(route.loaderQueryParams);
  assert.ok(route.setupController);

  assert.equal(route.entityType, "dag");
  assert.equal(route.loaderNamespace, "dags");

  assert.ok(route.filterRecords);

  assert.ok(route.actions.willTransition);
});

test('refresh test', function(assert) {
  let route = this.subject();

  assert.equal(route.get("queryParams.dagName.refreshModel"), true);
  assert.equal(route.get("queryParams.dagID.refreshModel"), true);
  assert.equal(route.get("queryParams.submitter.refreshModel"), true);
  assert.equal(route.get("queryParams.status.refreshModel"), true);
  assert.equal(route.get("queryParams.appID.refreshModel"), true);
  assert.equal(route.get("queryParams.callerID.refreshModel"), true);
  assert.equal(route.get("queryParams.rowCount.refreshModel"), true);
});

test('loaderQueryParams test', function(assert) {
  let route = this.subject();
  assert.equal(Object.keys(route.get("loaderQueryParams")).length, 7);
});

test('filterRecords test', function(assert) {
  let route = this.subject(),
      testRecords = [Ember.Object.create({
        name: "test"
      }), Ember.Object.create({
        // No name
      }),Ember.Object.create({
        name: "notest"
      })],
      testQuery = {
        dagName: "test"
      };

  let filteredRecords = route.filterRecords(testRecords, testQuery);

  assert.equal(filteredRecords.length, 1);
  assert.equal(filteredRecords[0], testRecords[0]);
});

test('load - query + filter test', function(assert) {
  let testEntityID1 = "entity_1",
      testEntityID2 = "entity_2",
      testEntityID3 = "entity_3",
      testSubmitter = "testSub",

      query = {
        limit: 5,
        submitter: testSubmitter
      },
      resultRecords = Ember.A([
        Ember.Object.create({
          submitter: testSubmitter,
          entityID: testEntityID1
        }),
        Ember.Object.create(),
        Ember.Object.create(),
        Ember.Object.create(),
        Ember.Object.create({
          submitter: testSubmitter,
          entityID: testEntityID2,
          status: "RUNNING"
        }),
        Ember.Object.create({
          submitter: testSubmitter,
          entityID: testEntityID3,
        })
      ]),

      route = this.subject({
        controller: Ember.Object.create()
      });

  route.loader = Ember.Object.create({
    query: function (type, query, options) {
      assert.equal(type, "dag");
      assert.equal(query.limit, 6);
      assert.equal(options.reload, true);
      return Ember.RSVP.resolve(resultRecords);
    },
    loadNeed: function (record, field, options) {
      assert.equal(record.get("entityID"), testEntityID2);
      assert.equal(field, "am");
      assert.equal(options.reload, true);
      return Ember.RSVP.resolve();
    }
  });

  assert.expect(3 + 3 + 2 + 3 + 2);

  assert.notOk(route.get("controller.moreAvailable"), "moreAvailable shouldn't be set!");
  assert.equal(route.get("fromId"), null, "fromId shouldn't be set");

  return route.load(null, query).then(function (records) {
    assert.equal(records.get("length"), 2, "Length should be 2!");
    assert.equal(records.get("0.entityID"), testEntityID1);
    assert.equal(records.get("1.entityID"), testEntityID2);

    assert.equal(route.get("controller.moreAvailable"), true, "moreAvailable was not set");
    assert.equal(route.get("fromId"), testEntityID3);
  });
});

test('actions.willTransition test', function(assert) {
  let route = this.subject({
    controller: Ember.Object.create()
  });

  route.set("loader", {
    unloadAll: function (type) {
      if(type === "dag" || type === "ahs-app") {
        assert.ok(true);
      }
      else {
        throw(new Error("Invalid type!"));
      }
    }
  });

  assert.expect(2);
  route.send("willTransition");
});
