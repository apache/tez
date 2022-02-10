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
import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';

module('Unit | Controller | dag/tasks', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let controller = this.owner.factoryFor('controller:dag/tasks').create({
      send() {},
      beforeSort: {bind() {}},
      initVisibleColumns() {},
      getCounterColumns: function () {
        return [];
      }
    });

    assert.ok(controller);
    assert.ok(controller.breadcrumbs);
    assert.ok(controller.columns);
    assert.equal(controller.columns.length, 8);
  });

  test('Log column test', function(assert) {
    let controller = this.owner.factoryFor('controller:dag/tasks').create({
          send() {},
          beforeSort: {bind() {}},
          initVisibleColumns() {},
          getCounterColumns: function () {
            return [];
          }
        }),
        testAttemptID = "attempt_1";

    var columnDef = controller.columns.findBy("id", "log"),
        getLogCellContent = columnDef.getCellContent;

    assert.equal(getLogCellContent(EmberObject.create()), undefined);

    assert.equal(getLogCellContent(EmberObject.create({
      successfulAttemptID: testAttemptID
    })), testAttemptID);

    assert.equal(getLogCellContent(EmberObject.create({
      attemptIDs: ["1", "2", testAttemptID]
    })), testAttemptID);

    assert.false(columnDef.get("enableSearch"));
  });
});
