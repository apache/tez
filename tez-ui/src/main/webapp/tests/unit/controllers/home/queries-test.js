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

import { run } from '@ember/runloop';
import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';

module('Unit | Controller | home/queries', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let controller = this.owner.factoryFor('controller:home/queries').create({
      send() {},
      initVisibleColumns() {}
    });

    assert.ok(controller);

    assert.ok(controller.queryParams);
    assert.equal(controller.queryParams.length, 9 + 5);

    assert.ok(controller.breadcrumbs);
    assert.ok(controller.headerComponentNames);
    assert.equal(controller.headerComponentNames.length, 3);
    assert.equal(controller.footerComponentNames.length, 1);

    assert.ok(controller.definition);
    assert.ok(controller.columns);
    assert.equal(controller.columns.length, 17);

    assert.ok(controller.getCounterColumns);

    assert.ok(controller.actions.search);
    assert.ok(controller.actions.pageChanged);

    assert.equal(controller.pageNum, 1);
  });

  test('definition test', function(assert) {
    let controller = this.owner.factoryFor('controller:home/queries').create({
          initVisibleColumns() {},
          beforeSort: {bind() {}},
          send() {}
        }),
        definition = controller.definition,

        testQueryID = "QueryID",
        testDagID = "DagID",
        testAppID = "AppID",
        testExecutionMode = "ExecutionMode",
        testUser = "User",
        testRequestUser = "RequestUser",
        testTablesRead = "TablesRead",
        testTablesWritten = "TablesWritten",
        testQueue = "queue",

        testPageNum = 10,
        testMoreAvailable = true,
        testLoadingMore = true;

    assert.equal(definition.get("queryID"), "");
    assert.equal(definition.get("dagID"), "");
    assert.equal(definition.get("appID"), "");
    assert.equal(definition.get("executionMode"), "");
    assert.equal(definition.get("user"), "");
    assert.equal(definition.get("requestUser"), "");
    assert.equal(definition.get("tablesRead"), "");
    assert.equal(definition.get("tablesWritten"), "");
    assert.equal(definition.get("queue"), "");

    assert.equal(definition.get("pageNum"), 1);

    assert.false(definition.get("moreAvailable"));
    assert.false(definition.get("loadingMore"));

    run(function () {
      controller.set("queryID", testQueryID);
      assert.equal(controller.get("definition.queryID"), testQueryID);

      controller.set("dagID", testDagID);
      assert.equal(controller.get("definition.dagID"), testDagID);

      controller.set("appID", testAppID);
      assert.equal(controller.get("definition.appID"), testAppID);

      controller.set("executionMode", testExecutionMode);
      assert.equal(controller.get("definition.executionMode"), testExecutionMode);

      controller.set("user", testUser);
      assert.equal(controller.get("definition.user"), testUser);

      controller.set("requestUser", testRequestUser);
      assert.equal(controller.get("definition.requestUser"), testRequestUser);

      controller.set("tablesRead", testTablesRead);
      assert.equal(controller.get("definition.tablesRead"), testTablesRead);

      controller.set("tablesWritten", testTablesWritten);
      assert.equal(controller.get("definition.tablesWritten"), testTablesWritten);

      controller.set("queue", testQueue);
      assert.equal(controller.get("definition.queue"), testQueue);

      controller.set("pageNum", testPageNum);
      assert.equal(controller.get("definition.pageNum"), testPageNum);

      controller.set("moreAvailable", testMoreAvailable);
      assert.equal(controller.get("definition.moreAvailable"), testMoreAvailable);

      controller.set("loadingMore", testLoadingMore);
      assert.equal(controller.get("definition.loadingMore"), testLoadingMore);
    });
  });

  test('breadcrumbs test', function(assert) {
    let breadcrumbs = this.owner.factoryFor('controller:home/queries').create({
      send() {},
      initVisibleColumns() {}
    }).get("breadcrumbs");

    assert.equal(breadcrumbs.length, 1);
    assert.equal(breadcrumbs[0].text, "All Queries");
  });

  test('getCounterColumns test', function(assert) {
    let getCounterColumns = this.owner.factoryFor('controller:home/queries').create({
      send() {},
      initVisibleColumns() {}
    }).get("getCounterColumns");

    assert.equal(getCounterColumns().length, 0);
  });
});
