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

import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { settled } from '@ember/test-helpers';
import EmberObject from '@ember/object';

module('Unit | Controller | abstract', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let controller = this.owner.factoryFor('controller:abstract').create({
      send() {},
      initVisibleColumns() {}
    });

    assert.ok(controller);
  });

  test('init test', async function(assert) {
    assert.expect(2);

    let controller = this.owner.factoryFor('controller:abstract').create({
      model: EmberObject.create(),
      send: function (name) {
        assert.true(name === "setBreadcrumbs" || name === 'bubbleBreadcrumbs');
      }
    });
    controller.set('model', EmberObject.create());
    await settled();
  });

  test('crumbObserver test', async function(assert) {
    assert.expect(2); // fired

    let controller = this.owner.factoryFor('controller:abstract').create({
      send: function (name) {
        assert.true(name === "setBreadcrumbs" || name === 'bubbleBreadcrumbs');
      }
    });
    controller.set("breadcrumbs", []);
    await settled();
  });

  test('setBreadcrumbs test', async function(assert) {
    let testName = "Abc", // Because all controllers are pointing to the leaf rout
        testBreadCrumbs = [];

    assert.expect(8);
    let controller = this.owner.factoryFor('controller:abstract').create({
      send: function (name, crumbs) {
        if (name === 'setBreadcrumbs') {
          assert.equal(name, "setBreadcrumbs");
          assert.ok(crumbs.hasOwnProperty(testName));
          assert.equal(crumbs[testName], testBreadCrumbs);
        }
        if (name === 'bubbleBreadcrumbs') {
          assert.equal(name, 'bubbleBreadcrumbs');
        }
      }
    });
    controller.set('model', EmberObject.create());
    controller.set('name', testName);
    controller.set('breadcrumbs', testBreadCrumbs);
    await settled();
  });
});
