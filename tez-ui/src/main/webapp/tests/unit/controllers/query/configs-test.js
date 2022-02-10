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

module('Unit | Controller | query/configs', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let controller = this.owner.factoryFor('controller:query/configs').create({
      send() {},
      initVisibleColumns() {}
    });

    assert.ok(controller);
    assert.equal(controller.searchText, "tez");
    assert.equal(controller.get("breadcrumbs.length"), 1);
    assert.equal(controller.get("columns.length"), 2);
  });

  test('Basic creation test', function(assert) {
    let controller = this.owner.factoryFor('controller:query/configs').create({
      send() {},
      initVisibleColumns() {},
      model: {
        configsJSON: JSON.stringify({
          x: 1
        })
      }
    });

    let configs = controller.configs;

    assert.equal(configs.length, 1);
    assert.equal(configs[0].configName, "x");
    assert.equal(configs[0].configValue, 1);
  });
});
