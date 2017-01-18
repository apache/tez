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

moduleForModel('hive-query', 'Unit | Model | hive query', {
  // Specify the other units that are required for this test.
  needs: []
});

test('Basic creation test', function(assert) {
  let model = this.subject();

  assert.ok(model);

  assert.ok(model.domain);

  assert.ok(model.user);
  assert.ok(model.requestUser);

  assert.ok(model.version);

  assert.ok(model.sessionID);
  assert.ok(model.threadName);

  assert.ok(model.queryText);

  assert.ok(model.configsJSON);

  assert.ok(model.startTime);
  assert.ok(model.endTime);
  assert.ok(model.duration);

  assert.ok(model.perf);
});

test('duration test', function(assert) {
  let model = this.subject();

  Ember.run(function () {
    model.set("startTime", 100);
    model.set("endTime", 200);
    assert.equal(model.get("duration"), 100);
  });
});