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

import { moduleFor, test } from 'ember-qunit';

moduleFor('route:application', 'Unit | Route | application', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let route = this.subject();

  assert.ok(route);
  assert.ok(route.pageReset);
  assert.ok(route.actions.didTransition);
  assert.ok(route.actions.bubbleBreadcrumbs);

  assert.ok(route.actions.error);

  assert.ok(route.actions.openModal);
  assert.ok(route.actions.closeModal);
  assert.ok(route.actions.destroyModal);

  assert.ok(route.actions.resetTooltip);
});

test('Test didTransition action', function(assert) {
  let route = this.subject();

  assert.expect(1);

  route.pageReset = function () {
    assert.ok(true);
  };

  route.send("didTransition");
});

test('Test bubbleBreadcrumbs action', function(assert) {
  let route = this.subject(),
      testController = {
        breadcrumbs: null
      },
      testBreadcrumbs = [{}];

  route.controller = testController;

  assert.notOk(route.get("controller.breadcrumbs"));
  route.send("bubbleBreadcrumbs", testBreadcrumbs);
  assert.equal(route.get("controller.breadcrumbs"), testBreadcrumbs);
});
