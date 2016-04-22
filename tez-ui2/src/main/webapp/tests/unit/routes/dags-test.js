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
          }
        }
      }),
      query = {
        limit: 5
      };

  assert.expect(3 + 1);

  route.load(null, query);
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
