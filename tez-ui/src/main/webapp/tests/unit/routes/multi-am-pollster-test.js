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

moduleFor('route:multi-am-pollster', 'Unit | Route | multi am pollster', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let route = this.subject();

  assert.ok(route);
  assert.ok(route.canPoll);
  assert.ok(route.actions.setPollingRecords);
});

test('canPoll test', function(assert) {
  let record = Ember.Object.create({
      }),
      route = this.subject({
        polling: {
          resetPoll: function () {}
        },
        _canPollObserver: function () {},
        polledRecords: Ember.A([record]),
        loadedValue: {}
      });

  assert.notOk(route.get("canPoll"));

  record.setProperties({
    app: Ember.Object.create({
      isComplete: false
    }),
    dag: undefined
  });
  assert.ok(route.get("canPoll"), true, "Test 1");

  record.set("app.isComplete", true);
  assert.notOk(route.get("canPoll"), "Test 2");

  record.set("app.isComplete", undefined);
  assert.notOk(route.get("canPoll"), "Test 3");

  record.set("dag", Ember.Object.create({
    isComplete: false
  }));
  assert.ok(route.get("canPoll"), "Test 4");

  record.set("dag.isComplete", true);
  assert.notOk(route.get("canPoll"), "Test 5");

  record.set("dag", undefined);
  assert.notOk(route.get("canPoll"), "Test 6");

  record.set("app.isComplete", false);
  assert.ok(route.get("canPoll"), "Test 7");
});
