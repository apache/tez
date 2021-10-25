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

module('Unit | Controller | query', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let controller = this.owner.factoryFor('controller:query').create({
      send() {},
      initVisibleColumns() {}
    });

    assert.ok(controller);
    assert.equal(controller.get("tabs.length"), 3);
  });

  test('breadcrumbs test', function(assert) {
    let testID = "test_1",
        controller = this.owner.factoryFor('controller:query').create({
          send() {},
          initVisibleColumns() {},
          model: EmberObject.create({
            entityID: testID
          })
        }),
        breadcrumbs = controller.breadcrumbs;

    assert.ok(breadcrumbs);
    assert.ok(breadcrumbs.length, 1);
    assert.ok(breadcrumbs[0].text, `Query [ ${testID} ]`);
  });
});
