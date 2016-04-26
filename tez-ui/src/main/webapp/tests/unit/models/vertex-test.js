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

import { moduleForModel, test } from 'ember-qunit';

moduleForModel('vertex', 'Unit | Model | vertex', {
  // Specify the other units that are required for this test.
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();

  assert.ok(model);

  assert.ok(model.needs.dag);
  assert.ok(model.needs.am);

  assert.ok(model.runningTasks);
  assert.ok(model.pendingTasks);

  assert.ok(model.initTime);
  assert.ok(model.startTime);
  assert.ok(model.endTime);
  assert.ok(model.firstTaskStartTime);
  assert.ok(model.lastTaskFinishTime);

  assert.ok(model.finalStatus);
});

test('runningTasks test', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    assert.equal(model.get("runningTasks"), null);
    model.set("status", "SUCCEEDED");
    assert.equal(model.get("runningTasks"), 0);
  });
});

test('pendingTasks test', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("totalTasks", null);
    assert.equal(model.get("pendingTasks"), null);
    model.set("totalTasks", 2);
    model.set("_succeededTasks", 1);
    model.set("status", "SUCCEEDED");
    assert.equal(model.get("pendingTasks"), 1);
  });
});
