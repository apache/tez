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

module('Unit | Route | home/index', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let route = this.owner.lookup('route:home/index');

    assert.ok(route);

    assert.equal(route.entityType, "dag");
    assert.equal(route.loaderNamespace, "dags");

  });

  test('refresh test', function(assert) {
    let route = this.owner.lookup('route:home/index');

    assert.true(route.get("queryParams.dagName.refreshModel"));
    assert.true(route.get("queryParams.dagID.refreshModel"));
    assert.true(route.get("queryParams.submitter.refreshModel"));
    assert.true(route.get("queryParams.status.refreshModel"));
    assert.true(route.get("queryParams.appID.refreshModel"));
    assert.true(route.get("queryParams.callerID.refreshModel"));
    assert.true(route.get("queryParams.rowCount.refreshModel"));
  });

  test('loaderQueryParams test', function(assert) {
    let route = this.owner.lookup('route:home/index');
    assert.equal(Object.keys(route.loaderQueryParams).length, 8);
  });

  test('filterRecords test', function(assert) {
    let route = this.owner.lookup('route:home/index'),
        testRecords = [EmberObject.create({
          name: "test"
        }), EmberObject.create({
          // No name
        }),EmberObject.create({
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
        resultRecords = A([
          EmberObject.create({
            submitter: testSubmitter,
            entityID: testEntityID1
          }),
          EmberObject.create(),
          EmberObject.create(),
          EmberObject.create(),
          EmberObject.create({
            submitter: testSubmitter,
            entityID: testEntityID2,
            status: "RUNNING"
          }),
          EmberObject.create({
            submitter: testSubmitter,
            entityID: testEntityID3,
          })
        ]),

        route = this.owner.factoryFor('route:home/index').create({
          controller: EmberObject.create()
        });

    route.loader = EmberObject.create({
      query: function (type, query, options) {
        assert.equal(type, "dag");
        assert.equal(query.limit, 6);
        assert.true(options.reload);
        return resolve(resultRecords);
      },
      loadNeed: function (record, field, options) {
        assert.equal(record.get("entityID"), testEntityID2);
        assert.equal(field, "am");
        assert.true(options.reload);
        return resolve();
      }
    });

    assert.expect(3 + 3 + 2 + 1 + 3 + 2);

    assert.notOk(route.get("controller.moreAvailable"), "moreAvailable shouldn't be set!");
    assert.equal(route.fromId, null, "fromId shouldn't be set");

    return route.load(null, query).then(function (records) {
      assert.ok(Array.isArray(records));

      assert.equal(records.get("length"), 2, "Length should be 2!");
      assert.equal(records.get("0.entityID"), testEntityID1);
      assert.equal(records.get("1.entityID"), testEntityID2);

      assert.true(route.get("controller.moreAvailable"), "moreAvailable was not set");
      assert.equal(route.fromId, testEntityID3);
    });
  });

  test.skip('actions.willTransition test', function(assert) {
    let route = this.owner.factoryFor('route:home/index').create({
      controller: EmberObject.create()
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

  test('actions.loadCounters test', function(assert) {
    let route = this.owner.factoryFor('route:home/index').create({
          controller: EmberObject.create()
        }),
        visibleRecords = [{}, {}, {}],
        index = 0;

    route.loader = {
      loadNeed: function (record, name) {
        assert.equal(record, visibleRecords[index++]);
        assert.equal(name, "info");
        return resolve(record);
      }
    };
    assert.expect(3 * 2);

    route.send("loadCounters");

    route.set("visibleRecords", visibleRecords);
    route.send("loadCounters");
  });
});
