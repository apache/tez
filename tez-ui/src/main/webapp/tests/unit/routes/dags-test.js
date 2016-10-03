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

moduleFor('route:dags', 'Unit | Route | dags', {
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
  assert.ok(route.load);

  assert.ok(route.filterRecords);

  assert.ok(route.loadNewPage);

  assert.ok(route.actions.setLoadTime);
  assert.ok(route.actions.loadPage);
  assert.ok(route.actions.reload);

  assert.ok(route.actions.willTransition);
});

test('filterRecords test', function(assert) {
  let route = this.subject(),
      testRecords = [Ember.Object.create({
        name: "test"
      }), Ember.Object.create({

      }),Ember.Object.create({
        name: "notest"
      })],
      testQuery = {
        dagName: "test"
      };

  let filteredRecords = route.filterRecords(testRecords, testQuery);

  assert.equal(filteredRecords.length, 1);
  assert.equal(filteredRecords[0].name, "test");
});

test('load test', function(assert) {
  let route = this.subject({
        filterRecords: function () {
          return [];
        },
        controller: Ember.Object.create(),
        loaderNamespace: undefined,
        loader: {
          query: function (type, query, options) {
            assert.equal(type, "dag");
            assert.equal(query.limit, 6);
            assert.equal(options.reload, true);
            return {
              then: function (callback) {
                callback(Ember.Object.create({
                  length: 6,
                  popObject: function () {
                    assert.ok(true);
                    return Ember.Object.create();
                  }
                }));
              }
            };
          },
          queryRecord: function (type, dagID, options) {
            assert.equal(type, "dag");
            assert.equal(options.reload, true);
            if (dagID === querySuccess.dagID) {
              return Ember.RSVP.resolve(Ember.Object.create());
            } else {
              return Ember.RSVP.reject(new Error("Failed in Reject"));
            }
          }
        }
      }),
      query = {
        limit: 5
      },
      querySuccess = {
        dagID :'dag_123'
      },
      queryFailure = {
        dagID :'dag_456'
      };

  assert.expect(8 + 2);

  route.load(null, query);
  route.load(null, querySuccess).then(function () {
    assert.ok(true);
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
      route = this.subject({
        controller: Ember.Object.create(),
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
          return {
            then: function (callback) {
              callback(data);
            }
          };
        }
      });

  assert.expect(1 + 2);

  route.loadNewPage();
});

test('actions.willTransition test', function(assert) {
  let testPageNum = 5,
      controller = Ember.Object.create({
        pageNum: testPageNum
      }),
      route = this.subject({
        controller: controller,
      });

  route.set("loader", {
    unloadAll: function () {
      assert.ok(true);
    }
  });

  assert.expect(2 + 1 + 1);

  assert.equal(controller.get("pageNum"), testPageNum);
  route.send("willTransition");
  assert.equal(controller.get("pageNum"), 1); // PageNum must be reset
});
