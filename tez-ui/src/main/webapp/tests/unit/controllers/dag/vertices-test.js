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

module('Unit | Controller | dag/vertices', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let controller = this.owner.factoryFor('controller:dag/vertices').create({
      send() {},
      beforeSort: {bind() {}},
      initVisibleColumns() {},
      getCounterColumns: function () {
        return [];
      }
    });

    assert.ok(controller);
    assert.ok(controller.breadcrumbs);
    assert.ok(controller.columns);
    assert.ok(controller.beforeSort);
  });

  test('beforeSort test', function(assert) {
    let controller = this.owner.factoryFor('controller:dag/vertices').create({
      initVisibleColumns() {},
      getCounterColumns: function () {
        return [];
      },
      polling: {
        isReady: true
      },
      send: function (actionName) {
        if(actionName === "openModal") {
          assert.ok(true);
        }
      }
    });

    // Bind poilyfill
    Function.prototype.bind = function (context) {
      var that = this;
      return function (val) {
        return that.call(context, val);
      };
    };

    assert.expect(1 + 3 + 3);

    assert.ok(controller.beforeSort(EmberObject.create({
      contentPath: "NonDisabledColumn"
    })), "NonDisabledColumn");

    assert.notOk(controller.beforeSort(EmberObject.create({
      contentPath: "succeededTasks"
    })), "succeededTasks");
    assert.notOk(controller.beforeSort(EmberObject.create({
      contentPath: "runningTasks"
    })), "runningTasks");
    assert.notOk(controller.beforeSort(EmberObject.create({
      contentPath: "pendingTasks"
    })), "pendingTasks");

  });
});
