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

moduleFor('controller:dags', 'Unit | Controller | dags', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  assert.expect(2 + 3 + 1 + 3 + 1 + 1);

  let controller = this.subject({
    initVisibleColumns: Ember.K,
    beforeSort: {bind: Ember.K},
    send: function (name, query) {
      assert.equal(name, "setBreadcrumbs");
      assert.ok(query);
    }
  });

  assert.ok(controller);
  assert.ok(controller.columns);
  assert.ok(controller.getCounterColumns);

  assert.ok(controller.pageNum);

  assert.ok(controller.queryParams);
  assert.ok(controller.headerComponentNames);
  assert.ok(controller.definition);

  assert.ok(controller.actions.search);
  assert.ok(controller.actions.pageChanged);
});
