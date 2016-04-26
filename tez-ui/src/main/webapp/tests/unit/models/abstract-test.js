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

moduleForModel('abstract', 'Unit | Model | abstract', {
  // Specify the other units that are required for this test.
  // needs: []
});

test('Basic test for existence', function(assert) {
  let model = this.subject();

  assert.ok(model);
  assert.ok(model.mergedProperties);
  assert.ok(model.refreshLoadTime);

  assert.ok(model._notifyProperties);
  assert.ok(model.didLoad);

  assert.ok(model.entityID);
  assert.ok(model.index);
  assert.ok(model.status);
  assert.ok(model.isComplete);
});

test('_notifyProperties test - will fail if _notifyProperties implementation is changed in ember-data', function(assert) {
  let model = this.subject();

  Ember._beginPropertyChanges = Ember.beginPropertyChanges;

  assert.expect(1 + 1);
  // refreshLoadTime will be called by us & beginPropertyChanges by ember data

  Ember.beginPropertyChanges = function () {
    assert.ok(true);
    Ember._beginPropertyChanges();
  };
  model.refreshLoadTime = function () {
    assert.ok(true);
  };

  model._notifyProperties([]);

  Ember.beginPropertyChanges = Ember._beginPropertyChanges;
});
