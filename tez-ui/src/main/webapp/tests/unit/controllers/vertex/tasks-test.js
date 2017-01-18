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

moduleFor('controller:vertex/tasks', 'Unit | Controller | vertex/tasks', {
  // Specify the other units that are required for this test.
  // needs: ['service:local-storage']
});

test('Basic creation test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    beforeSort: {bind: Ember.K},
    initVisibleColumns: Ember.K,
    getCounterColumns: function () {
      return [];
    }
  });

  assert.ok(controller);
  assert.ok(controller.breadcrumbs);
  assert.ok(controller.columns);
  assert.equal(controller.columns.length, 7);
});

test('Log column test', function(assert) {
  let controller = this.subject({
        send: Ember.K,
        beforeSort: {bind: Ember.K},
        initVisibleColumns: Ember.K,
        getCounterColumns: function () {
          return [];
        }
      }),
      testAttemptID = "attempt_1";

  var getLogCellContent = controller.get("columns").findBy("id", "log").getCellContent;

  assert.equal(getLogCellContent(Ember.Object.create()), undefined);

  assert.equal(getLogCellContent(Ember.Object.create({
    successfulAttemptID: testAttemptID
  })), testAttemptID);

  assert.equal(getLogCellContent(Ember.Object.create({
    attemptIDs: ["1", "2", testAttemptID]
  })), testAttemptID);
});