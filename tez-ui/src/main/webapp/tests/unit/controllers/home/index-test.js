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

module('Unit | Controller | home/index', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    assert.expect(2 + 4 + 1 + 4 + 1);

    let controller = this.owner.factoryFor('controller:home/index').create({
      initVisibleColumns() {},
      beforeSort: {bind() {}},
      send: function (name, query) {
        assert.equal(name, "setBreadcrumbs");
        assert.ok(query);
      }
    });

    assert.ok(controller);
    assert.ok(controller.columns);
    assert.ok(controller.columns.length, 13);
    assert.ok(controller.getCounterColumns);

    assert.ok(controller.pageNum);

    assert.ok(controller.queryParams);
    assert.ok(controller.headerComponentNames);
    assert.equal(controller.headerComponentNames.length, 3);
    assert.equal(controller.footerComponentNames.length, 2);

    assert.ok(controller._definition);
    assert.ok(controller.definition);

    assert.ok(controller.actions.pageChanged);
  });

  test('queryParams test', function(assert) {
    let controller = this.owner.factoryFor('controller:home/index').create({
          initVisibleColumns() {},
          beforeSort: {bind() {}},
          send() {}
        });

    // 11 New, 5 Inherited & 4 for backward compatibility
    assert.equal(controller.get("queryParams.length"), 7 + 5 + 4);
  });

  test('definition test', function(assert) {
    let controller = this.owner.factoryFor('controller:home/index').create({
          initVisibleColumns() {},
          beforeSort: {bind() {}},
          send() {}
        }),
        definition = controller.definition,
        testDAGName = "DAGName",
        testDAGID = "DAGID",
        testSubmitter = "Submitter",
        testStatus = "Status",
        testAppID = "AppID",
        testCallerID = "CallerID",
        testQueue = "Queue",
        testPageNum = 10,
        testMoreAvailable = true,
        testLoadingMore = true;

    assert.equal(definition.get("dagName"), "");
    assert.equal(definition.get("dagID"), "");
    assert.equal(definition.get("submitter"), "");
    assert.equal(definition.get("status"), "");
    assert.equal(definition.get("appID"), "");
    assert.equal(definition.get("callerID"), "");
    assert.equal(definition.get("queue"), "");

    assert.equal(definition.get("pageNum"), 1);

    assert.false(definition.get("moreAvailable"));
    assert.false(definition.get("loadingMore"));

    run(function () {
      controller.set("dagName", testDAGName);
      assert.equal(controller.get("definition.dagName"), testDAGName);

      controller.set("dagID", testDAGID);
      assert.equal(controller.get("definition.dagID"), testDAGID);

      controller.set("submitter", testSubmitter);
      assert.equal(controller.get("definition.submitter"), testSubmitter);

      controller.set("status", testStatus);
      assert.equal(controller.get("definition.status"), testStatus);

      controller.set("appID", testAppID);
      assert.equal(controller.get("definition.appID"), testAppID);

      controller.set("callerID", testCallerID);
      assert.equal(controller.get("definition.callerID"), testCallerID);

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
    let breadcrumbs = this.owner.factoryFor('controller:home/index').create({
      initVisibleColumns() {},
      beforeSort: {bind() {}},
      send() {}
    }).get("breadcrumbs");

    assert.equal(breadcrumbs.length, 1);
    assert.equal(breadcrumbs[0].text, "All DAGs");
  });
});
