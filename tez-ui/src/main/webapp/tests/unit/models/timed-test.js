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

moduleForModel('timed', 'Unit | Model | timed', {
  // Specify the other units that are required for this test.
  needs: []
});

test('it exists', function(assert) {
  let model = this.subject();

  assert.ok(model);
  assert.ok(model.startTime);
  assert.ok(model.duration);
  assert.ok(model.endTime);
});

test('duration test', function(assert) {
  let model = this.subject();

  function resetAndCheckModel () {
    model.set("startTime", 100);
    model.set("endTime", 200);

    assert.equal(model.get("duration"), 100);
  }

  Ember.run(function () {
    resetAndCheckModel();
    model.set("endTime", 100);
    assert.equal(model.get("duration"), 0);

    model.set("startTime", 0);
    assert.equal(model.get("duration").message, "Start time is less than or equal to zero!");

    resetAndCheckModel();
    model.set("endTime", 0);
    assert.equal(model.get("duration").message, "End time is less than or equal to zero!");

    resetAndCheckModel();
    model.set("endTime", 50);
    assert.equal(model.get("duration").message, "Start time is greater than end time by 50 msecs!");

    resetAndCheckModel();
    model.set("startTime", -100);
    assert.equal(model.get("duration").message, "Start time is less than or equal to zero!");

    resetAndCheckModel();
    model.set("endTime", -200);
    assert.equal(model.get("duration").message, "End time is less than or equal to zero!");

    resetAndCheckModel();
    model.set("startTime", undefined);
    assert.equal(model.get("duration"), undefined);

    resetAndCheckModel();
    model.set("endTime", undefined);
    assert.equal(model.get("duration"), undefined);

    resetAndCheckModel();
    model.set("startTime", null);
    assert.equal(model.get("duration"), undefined);

    resetAndCheckModel();
    model.set("endTime", null);
    assert.equal(model.get("duration"), undefined);
  });
});
