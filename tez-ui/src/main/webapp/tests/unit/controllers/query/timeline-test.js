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

moduleFor('controller:query/timeline', 'Unit | Controller | query/timeline', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K
  });

  assert.ok(controller);

  assert.ok(controller.columns);
  assert.equal(controller.columns.length, 2);

  assert.ok(controller.rows);
});

test('rows test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K,
    model: {
      perf: {
        x: 1,
        y: 2,
        z: 3
      }
    }
  }),
  rows = controller.get("rows");

  assert.equal(rows[0].perfLogName, "x");
  assert.equal(rows[0].perfLogValue, 1);

  assert.equal(rows[1].perfLogName, "y");
  assert.equal(rows[1].perfLogValue, 2);

  assert.equal(rows[2].perfLogName, "z");
  assert.equal(rows[2].perfLogValue, 3);
});