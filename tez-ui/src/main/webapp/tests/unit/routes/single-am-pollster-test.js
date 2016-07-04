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

moduleFor('route:single-am-pollster', 'Unit | Route | single am pollster', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let route = this.subject();

  assert.ok(route);
  assert.ok(route.canPoll);
  assert.ok(route._loadedValueObserver);
});

test('canPoll test', function(assert) {
  let route = this.subject({
    polling: {
      resetPoll: function () {}
    },
    _canPollObserver: function () {}
  });

  assert.notOk(route.get("canPoll"));

  route.setProperties({
    polledRecords: {},
    loadedValue: {
      app: {
        isComplete: false
      }
    }
  });
  assert.ok(route.get("canPoll"), true);

  route.set("loadedValue.app.isComplete", true);
  assert.notOk(route.get("canPoll"));

  route.set("loadedValue.app.isComplete", undefined);
  assert.notOk(route.get("canPoll"));
});

test('_loadedValueObserver test', function(assert) {
  let route = this.subject({
    polling: {
      resetPoll: function () {}
    },
    _canPollObserver: function () {}
  }),
  loadedValue = Ember.Object.create();

  assert.equal(route.get("polledRecords"), null);

  route.set("loadedValue", loadedValue);
  assert.equal(route.get("polledRecords.0"), loadedValue);

  route.set("polledRecords", null);

  loadedValue.set("loadTime", 1);
  assert.equal(route.get("polledRecords.0"), loadedValue);
});
