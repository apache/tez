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
import { resolve, reject } from 'rsvp';

module('Unit | Route | server side ops', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let route = this.owner.lookup('route:server-side-ops');

    assert.ok(route);
  });

  test('load - query/filter test', function(assert) {
    let testEntityType = "EntityType",
        testEntityID1 = "entity_1",
        testEntityID2 = "entity_2",
        testFromID = "entity_6",

        query = {
          limit: 5
        },
        resultRecords = A([
          EmberObject.create({
            entityID: testEntityID1
          }),
          {}, {}, {}, {},
          EmberObject.create({
            entityID: testFromID
          })
        ]),

        route = this.owner.factoryFor('route:server-side-ops').create({
          entityType: testEntityType,
          controller: EmberObject.create(),
          loader: {
            query: function (type, query, options) {
              assert.equal(type, testEntityType);
              assert.equal(query.limit, 6);
              assert.true(options.reload);
              return resolve(resultRecords);
            }
          }
        });

    assert.expect(3 * 2 + 2 + 3 + 3);

    assert.notOk(route.get("controller.moreAvailable"));
    assert.equal(route.fromId, null);

    return route.load(null, query).then(function (records) {
      assert.equal(records.get("0.entityID"), testEntityID1);

      assert.true(route.get("controller.moreAvailable"), "moreAvailable was not set");
      assert.equal(route.fromId, testFromID);
    }).then(function () {
      resultRecords = A([
        EmberObject.create({
          entityID: testEntityID2
        })
      ]);
      return route.load(null, query);
    }).then(function (records) {
      assert.equal(records.get("0.entityID"), testEntityID2);

      assert.false(route.get("controller.moreAvailable"));
      assert.equal(route.fromId, null);
    });
  });

  test('load - id fetch test', function(assert) {
    let testEntityType = "EntityType",
        testRecord = EmberObject.create(),
        route = this.owner.factoryFor('route:server-side-ops').create({
          entityType: testEntityType,
          controller: EmberObject.create(),
          loader: {
            queryRecord: function (type, id, options) {
              assert.equal(type, testEntityType);
              assert.true(options.reload);
              if (id === querySuccess.id) {
                return resolve(testRecord);
              } else {
                return reject(new Error("Failed in Reject"));
              }
            }
          }
        }),
        querySuccess = {
          id :'entity_123'
        },
        queryFailure = {
          id :'entity_456'
        };

    assert.expect(2 * 2 + 3 + 1);

    route.load(null, querySuccess).then(function (records) {
      assert.ok(Array.isArray(records));
      assert.equal(records.length, 1);
      assert.equal(records[0], testRecord);
    });
    route.load(null, queryFailure).then(function (data) {
      assert.equal(data.length,0);
    });
  });

  test('loadNewPage test', function(assert) {
    let currentQuery = {
          val: {}
        },
        data = [],
        fromId = "id1",
        route = this.owner.factoryFor('route:server-side-ops').create({
          controller: EmberObject.create(),
          currentQuery: currentQuery,
          fromId: fromId,
          loadedValue: {
            pushObjects: function (objs) {
              assert.equal(data, objs);
            }
          },
          load: function (value, query) {
            assert.equal(query.val, currentQuery.val);
            assert.equal(query.fromId, fromId);
            return resolve(data);
          }
        });

    assert.expect(1 + 2);

    route.loadNewPage();
  });

  test('actions.willTransition test', function(assert) {
    let testPageNum = 5,
        controller = EmberObject.create({
          pageNum: testPageNum
        }),
        route = this.owner.factoryFor('route:server-side-ops').create({
          controller: controller,
        });

    assert.expect(1 + 1);

    assert.equal(controller.pageNum, testPageNum);
    route.send("willTransition");
    assert.equal(controller.pageNum, 1); // PageNum must be reset
  });
});
