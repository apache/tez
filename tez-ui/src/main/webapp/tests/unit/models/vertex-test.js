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

import EmberObject from '@ember/object';
import { run } from '@ember/runloop';
import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';

module('Unit | Model | vertex', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('vertex'));

    assert.ok(model);
  });

  test('runningTasks test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('vertex'));

    run(function () {
      assert.equal(model.get("runningTasks"), null);
      model.set("status", "SUCCEEDED");
      assert.equal(model.get("runningTasks"), 0);
    });
  });

  test('pendingTasks test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('vertex'));

    run(function () {
      model.set("totalTasks", null);
      assert.equal(model.get("pendingTasks"), null);
      model.set("totalTasks", 2);
      model.set("_succeededTasks", 1);
      model.set("status", "SUCCEEDED");
      assert.equal(model.get("pendingTasks"), 1);
    });
  });

  test('description test', function(assert) {
    let testVertexName = "TestVertexName",
        testDesc = "VertexDecsription",
        model = run(() => this.owner.lookup('service:store').createRecord('vertex', {
          name: testVertexName
        }));

    assert.equal(model.get("description"), undefined);

    run(function () {
      model.set("dag", EmberObject.create({
        vertices: [{}, {
          vertexName: testVertexName,
          userPayloadAsText: JSON.stringify({
            desc: testDesc
          })
        }, {}]
      }));
      assert.equal(model.get("description"), testDesc);
    });
  });
});
