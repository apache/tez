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

module('Unit | Model | abstract', function(hooks) {
  setupTest(hooks);

  test('Basic test for existence', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('abstract'));

    assert.ok(model);
  });

  test('isComplete test', function(assert) {
    let model = run(() => this.owner.lookup('service:store').createRecord('abstract'));
    assert.false(model.get("isComplete"));

    run(function () {
      model.set("status", "SUCCEEDED");
      assert.true(model.get("isComplete"));

      model.set("status", null);
      assert.false(model.get("isComplete"));
      model.set("status", "FINISHED");
      assert.true(model.get("isComplete"));

      model.set("status", null);
      model.set("status", "FAILED");
      assert.true(model.get("isComplete"));

      model.set("status", null);
      model.set("status", "KILLED");
      assert.true(model.get("isComplete"));

      model.set("status", null);
      model.set("status", "ERROR");
      assert.true(model.get("isComplete"));
    });
  });
});
