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

module('Unit | Controller | task/attempts', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let controller = this.owner.factoryFor('controller:task/attempts').create({
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

    assert.equal(controller.get("columns.length"), 10);
  });

  test('Log column test', function(assert) {
    let controller = this.owner.factoryFor('controller:task/attempts').create({
      send() {},
      beforeSort: {bind() {}},
      initVisibleColumns() {},
      getCounterColumns: function () {
        return [];
      }
    }),
    url = "http://abc.com",
    logColumnDef = controller.columns.findBy('id', 'log'),
    content;

    assert.notOk(logColumnDef.getCellContent(EmberObject.create()));

    content = logColumnDef.getCellContent(EmberObject.create({
      logURL: url
    }));
    assert.equal(content[0].href, url);
    assert.equal(content[0].text, "View");
    assert.equal(content[1].href, url);
    assert.equal(content[1].text, "Download");
    assert.true(content[1].download);
  });
});
