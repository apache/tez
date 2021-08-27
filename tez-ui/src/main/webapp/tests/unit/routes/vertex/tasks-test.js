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
import { resolve } from 'rsvp';

module('Unit | Route | vertex/tasks', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let route = this.owner.factoryFor('route:vertex/tasks').create({
      initVisibleColumns() {},
      getCounterColumns() {}
    });

    assert.ok(route);
  });

  test('setupController test', function(assert) {
    assert.expect(1);

    let route = this.owner.factoryFor('route:vertex/tasks').create({
      startCrumbBubble: function () {
        assert.ok(true);
      }
    });

    route.setupController({}, {});
  });

  test('logCellClicked test', function(assert) {
    assert.expect(2 * 3 + 2 + 2 + 1);

    let testID = "attempt_1",
        testLogURL = "http://abc.com/xyz",
        route = this.owner.lookup('route:vertex/tasks'),
        attemptRecord = EmberObject.create({
          logURL: testLogURL,
          entityID: testID
        });

    route.loader = {
      queryRecord: function (type, id) {
        assert.equal(type, "attempt");
        assert.equal(id, testID);

        return resolve(attemptRecord);
      }
    };
    route.send = function (actionName) {
      assert.equal(actionName, "openModal");
    };

    // Download false
    route.actions.logCellClicked.call(route, testID, false).then(function (virtualAnchorInstance) {
      assert.equal(virtualAnchorInstance.href, testLogURL);
      assert.notOk(virtualAnchorInstance.download);
    });

    // Download true
    route.actions.logCellClicked.call(route, testID, true).then(function (virtualAnchorInstance) {
      assert.equal(virtualAnchorInstance.href, testLogURL);
      assert.equal(virtualAnchorInstance.download, testID);
    });

    // No log
    attemptRecord = EmberObject.create();
    route.actions.logCellClicked.call(route, testID, true);
  });
});
