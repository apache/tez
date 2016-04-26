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

moduleFor('controller:abstract', 'Unit | Controller | abstract', {
  // Specify the other units that are required for this test.
  // needs: ['route:abstract']
});

test('Basic creation test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K
  });

  assert.ok(controller.name);
  assert.ok(controller.crumbObserver);
  assert.ok(controller.setBreadcrumbs);
  assert.ok(controller.loaded);
});

test('init test', function(assert) {
  assert.expect(1);

  this.subject({
    send: function (name) {
      assert.equal(name, "setBreadcrumbs");
    }
  });
});

test('crumbObserver test', function(assert) {
  assert.expect(1 + 1); // Init and fired

  let controller = this.subject({
    send: function (name) {
      assert.equal(name, "setBreadcrumbs");
    }
  });

  controller.set("breadcrumbs", []);
});

test('setBreadcrumbs test', function(assert) {
  let testName = "Abc", // Because all controllers are pointing to the leaf rout
      testBreadCrumbs = [];

  assert.expect(3);
  this.subject({
    name: testName,
    breadcrumbs: testBreadCrumbs,
    send: function (name, crumbs) {
      assert.equal(name, "setBreadcrumbs");
      assert.ok(crumbs.hasOwnProperty(testName));
      assert.equal(crumbs[testName], testBreadCrumbs);
    }
  });
});
