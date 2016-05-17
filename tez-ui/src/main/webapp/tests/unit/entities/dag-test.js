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

moduleFor('entitie:dag', 'Unit | Entity | dag', {
  // Specify the other units that are required for this test.
  // needs: ['entitie:foo']
});

test('Basic creation test', function(assert) {
  let entity = this.subject();
  assert.ok(entity);
  assert.ok(entity.queryRecord);
});

test('queryRecord-Hive:No description test', function(assert) {
  let testQuery = "testQuery",
      entityName = "entity-name",
      testDag = Ember.Object.create({
        name: "testName:1"
      }),
      hiveQuery = Ember.Object.create({
        queryText: testQuery
      }),
      store = {
        queryRecord: function (name) {
          assert.equal(name, entityName);
          return Ember.RSVP.resolve(testDag);
        }
      },
      loader = Ember.Object.create({
        nameSpace: "ns",
        queryRecord: function (type, id/*, options, query, urlParams*/) {
          assert.equal(type, "hive-query");
          assert.equal(id, "testName");
          return Ember.RSVP.resolve(hiveQuery);
        }
      }),
      entity = this.subject({
        name: entityName,
        store: store
      });

  assert.expect(1 + 2 + 3);

  entity.queryRecord(loader).then(function (dag) {
    assert.equal(testDag, dag);
    assert.equal(testDag.get("callerContext"), "Hive");
    assert.equal(testDag.get("callerDescription"), testQuery);
  });
});

test('queryRecord-Not Hive:No description test', function(assert) {
  let testQuery = "testQuery",
      entityName = "entity-name",
      testDag = Ember.Object.create({
        name: "testName"
      }),
      hiveQuery = Ember.Object.create({
        queryText: testQuery
      }),
      store = {
        queryRecord: function (name) {
          assert.equal(name, entityName);
          return Ember.RSVP.resolve(testDag);
        }
      },
      loader = Ember.Object.create({
        nameSpace: "ns",
        queryRecord: function (/*type, id, options, query, urlParams*/) {
          assert.ok(false);
          return Ember.RSVP.resolve(hiveQuery);
        }
      }),
      entity = this.subject({
        name: entityName,
        store: store
      });

  assert.expect(1 + 3);

  entity.queryRecord(loader).then(function (dag) {
    assert.equal(testDag, dag);
    assert.ok(!testDag.get("callerContext"));
    assert.ok(!testDag.get("callerDescription"));
  });
});

test('queryRecord-With description test', function(assert) {
  let testQuery = "testQuery",
      entityName = "entity-name",
      testDag = Ember.Object.create({
        name: "testName",
        callerDescription: testQuery
      }),
      loader = Ember.Object.create({
        nameSpace: "ns",
        queryRecord: function (/*type, id, options, query, urlParams*/) {
          assert.ok(false);
        }
      }),
      store = {
        queryRecord: function (name) {
          assert.equal(name, entityName);
          return Ember.RSVP.resolve(testDag);
        }
      },
      entity = this.subject({
        name: entityName,
        store: store
      });

  assert.expect(1 + 2);

  entity.queryRecord(loader).then(function (dag) {
    assert.equal(testDag, dag);
    assert.equal(testDag.get("callerDescription"), testQuery);
  });
});
