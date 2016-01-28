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

moduleForModel('timeline', 'Unit | Model | timeline', {
  // Specify the other units that are required for this test.
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();

  assert.ok(!!model);

  assert.ok(model.needs);

  assert.ok(model.entityID);
  assert.ok(model.appID);
  assert.ok(model.app);

  assert.ok(model.atsStatus);
  assert.ok(model.status);
  assert.ok(model.progress);

  assert.ok(model.startTime);
  assert.ok(model.endTime);
  assert.ok(model.duration);

  assert.ok(model._counterGroups);
  assert.ok(model.counterGroupsHash);
});

test('appID test', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("entityID", "a_1_2_3");
    assert.equal(model.get("appID"), "application_1_2");
  });
});

test('status test', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("atsStatus", "RUNNING");
    assert.equal(model.get("status"), "RUNNING");

    model.set("app", {
      status: "FAILED"
    });
    assert.equal(model.get("status"), "FAILED");
  });
});

test('progress test', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("status", "RUNNING");
    assert.equal(model.get("progress"), null);

    model.set("status", "SUCCEEDED");
    assert.equal(model.get("progress"), 1);
  });
});

test('duration test', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("startTime", 100);
    model.set("endTime", 200);
    assert.equal(model.get("duration"), 100);
  });
});

test('counterGroupsHash test', function(assert) {
  let model = this.subject(),
      testCounterGroup = [{
        counterGroupName: "group_1",
        counters: [{
          counterName: "counter_1_1",
          counterValue: "value_1_1"
        },{
          counterName: "counter_1_2",
          counterValue: "value_1_2"
        }]
      },{
        counterGroupName: "group_2",
        counters: [{
          counterName: "counter_2_1",
          counterValue: "value_2_1"
        },{
          counterName: "counter_2_2",
          counterValue: "value_2_2"
        }]
      }];

  Ember.run(function () {
    model.set("_counterGroups", testCounterGroup);
    assert.equal(model.get("counterGroupsHash.group_1.counter_1_1"), "value_1_1");
    assert.equal(model.get("counterGroupsHash.group_1.counter_1_2"), "value_1_2");
    assert.equal(model.get("counterGroupsHash.group_2.counter_2_1"), "value_2_1");
    assert.equal(model.get("counterGroupsHash.group_2.counter_2_2"), "value_2_2");
  });
});
