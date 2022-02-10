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

import EmberObject from '@ember/object';
import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { resolve } from 'rsvp';

module('Unit | Entity | dag', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let entity = this.owner.lookup('entitie:dag');
    assert.ok(entity);
    assert.ok(entity.queryRecord);
  });

  test('queryRecord-Hive:No description test', function(assert) {
    let testQuery = "testQuery",
        entityName = "entity-name",
        testDag = EmberObject.create({
          refreshLoadTime () {},
          name: "testName:1"
        }),
        hiveQuery = EmberObject.create({
          refreshLoadTime() {},
          queryText: testQuery
        }),
        store = {
          queryRecord: function (name) {
            assert.equal(name, entityName);
            return resolve(testDag);
          }
        },
        loader = EmberObject.create({
          nameSpace: "ns",
          queryRecord: function (type, id/*, options, query, urlParams*/) {
            assert.equal(type, "hive-query");
            assert.equal(id, "testName");
            return resolve(hiveQuery);
          }
        }),
        entity = this.owner.factoryFor('entitie:dag').create({
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
        testDag = EmberObject.create({
          refreshLoadTime () {},
          name: "testName"
        }),
        hiveQuery = EmberObject.create({
          refreshLoadTime () {},
          queryText: testQuery
        }),
        store = {
          queryRecord: function (name) {
            assert.equal(name, entityName);
            return resolve(testDag);
          }
        },
        loader = EmberObject.create({
          nameSpace: "ns",
          queryRecord: function (/*type, id, options, query, urlParams*/) {
            assert.ok(false);
            return resolve(hiveQuery);
          }
        }),
        entity = this.owner.factoryFor('entitie:dag').create({
          name: entityName,
          store: store
        });

    assert.expect(1 + 3);

    entity.queryRecord(loader).then(function (dag) {
      assert.equal(testDag, dag);
      assert.notOk(testDag.get("callerContext"));
      assert.notOk(testDag.get("callerDescription"));
    });
  });

  test('queryRecord-With description test', function(assert) {
    let testQuery = "testQuery",
        entityName = "entity-name",
        testDag = EmberObject.create({
          refreshLoadTime () {},
          name: "testName",
          callerDescription: testQuery
        }),
        loader = EmberObject.create({
          nameSpace: "ns",
          queryRecord: function (/*type, id, options, query, urlParams*/) {
            assert.ok(false);
          }
        }),
        store = {
          queryRecord: function (name) {
            assert.equal(name, entityName);
            return resolve(testDag);
          }
        },
        entity = this.owner.factoryFor('entitie:dag').create({
          name: entityName,
          store: store
        });

    assert.expect(1 + 2);

    entity.queryRecord(loader).then(function (dag) {
      assert.equal(testDag, dag);
      assert.equal(testDag.get("callerDescription"), testQuery);
    });
  });
});
