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
import { render, settled, findAll } from '@ember/test-helpers';
import { setupRenderingTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { hbs } from 'ember-cli-htmlbars';

module('Integration | Component | queries page search', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {
    await render(hbs`{{queries-page-search}}`);
    assert.equal(findAll("input").length, 8);

    // Template block usage:" + EOL +
    await render(hbs`
      {{#queries-page-search}}
        template block text
      {{/queries-page-search}}
    `);
    assert.equal(findAll("input").length, 8);
  });

  test('tableDefinition test', async function(assert) {
    var testQueryID = "query_1",
        testUser = "user",
        testTablesRead = "TablesRead",
        testTablesWritten = "TablesWritten",
        testAppID = "AppID",
        testDagID = "DAGID",
        testQueue = "queue",
        testExecutionMode = "ExecutionMode";

    this.set("tableDefinition", EmberObject.create({
      queryID: testQueryID,
      requestUser: testUser,
      tablesRead: testTablesRead,
      tablesWritten: testTablesWritten,
      appID: testAppID,
      dagID: testDagID,
      queue: testQueue,
      executionMode: testExecutionMode,
    }));

    await render(hbs`{{queries-page-search tableDefinition=tableDefinition}}`);

    return settled().then(() => {
      let inputs = findAll('input');
      assert.equal(inputs.length, 8);
      assert.equal(inputs[0].value, testQueryID);
      assert.equal(inputs[1].value, testUser);
      assert.equal(inputs[2].value, testDagID);
      assert.equal(inputs[3].value, testTablesRead);
      assert.equal(inputs[4].value, testTablesWritten);
      assert.equal(inputs[5].value, testAppID);
      assert.equal(inputs[6].value, testQueue);
      assert.equal(inputs[7].value, testExecutionMode);
    });
  });
});
