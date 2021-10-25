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

import { run } from '@ember/runloop';
import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';

module('Unit | Model | timeline', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('timeline'));

    assert.ok(model);
  });

  test('appID test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('timeline'));

    run(function () {
      model.set("entityID", "a_1_2_3");
      assert.equal(model.get("appID"), "application_1_2");
    });
  });

  test('status test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('timeline'));

    run(function () {
      model.set("atsStatus", "RUNNING");
      assert.equal(model.get("status"), "RUNNING");

      model.set("app", {
        status: "FAILED"
      });
      assert.equal(model.get("status"), "FAILED");
    });
  });

  test('progress test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('timeline'));

    run(function () {
      model.set("status", "RUNNING");
      assert.equal(model.get("progress"), null);

      model.set("status", "SUCCEEDED");
      assert.equal(model.get("progress"), 1);
    });
  });

  test('counterGroupsHash test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('timeline')),
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

    run(function () {
      model.set("_counterGroups", testCounterGroup);
      assert.equal(model.get("counterGroupsHash.group_1.counter_1_1"), "value_1_1");
      assert.equal(model.get("counterGroupsHash.group_1.counter_1_2"), "value_1_2");
      assert.equal(model.get("counterGroupsHash.group_2.counter_2_1"), "value_2_1");
      assert.equal(model.get("counterGroupsHash.group_2.counter_2_2"), "value_2_2");
    });
  });
});
