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

moduleFor('controller:table', 'Unit | Controller | table', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    initVisibleColumns: Ember.K
  });

  assert.ok(controller);
  assert.ok(controller.queryParams);

  assert.equal(controller.rowCount, 10);
  assert.equal(controller.searchText, "");
  assert.equal(controller.sortColumnId, "");
  assert.equal(controller.sortOrder, "");
  assert.equal(controller.pageNo, 1);

  assert.ok(controller.headerComponentNames);
  assert.ok(controller.visibleColumnIDs);
  assert.ok(controller.columnSelectorTitle);
  assert.ok(controller.definition);

  assert.ok(controller.storageID);
  assert.ok(controller.initVisibleColumns);

  assert.ok(controller.beforeSort);
  assert.ok(controller.columns);
  assert.ok(controller.allColumns);
  assert.ok(controller.visibleColumns);

  assert.ok(controller.getCounterColumns);

  assert.ok(controller.actions.searchChanged);
  assert.ok(controller.actions.sortChanged);
  assert.ok(controller.actions.rowsChanged);
  assert.ok(controller.actions.pageChanged);

  assert.ok(controller.actions.openColumnSelector);
  assert.ok(controller.actions.columnsSelected);
});

test('initVisibleColumns test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    localStorage: Ember.Object.create(),
    columns: []
  });

  controller.set("columns", [{
    id: "c1",
  }, {
    id: "c2",
  }, {
    id: "c3",
  }]);
  controller.initVisibleColumns();
  assert.equal(controller.get("visibleColumnIDs.c1"), true);
  assert.equal(controller.get("visibleColumnIDs.c2"), true);
  assert.equal(controller.get("visibleColumnIDs.c3"), true);

  controller.set("columns", [{
    id: "c1",
    hiddenByDefault: true,
  }, {
    id: "c2",
  }, {
    id: "c3",
    hiddenByDefault: true,
  }]);
  controller.initVisibleColumns();
  assert.equal(controller.get("visibleColumnIDs.c1"), false);
  assert.equal(controller.get("visibleColumnIDs.c2"), true);
  assert.equal(controller.get("visibleColumnIDs.c3"), false);

  controller.initVisibleColumns();
  assert.equal(controller.get("visibleColumnIDs.c1"), false);
  assert.equal(controller.get("visibleColumnIDs.c2"), true);
  assert.equal(controller.get("visibleColumnIDs.c3"), false);
});
