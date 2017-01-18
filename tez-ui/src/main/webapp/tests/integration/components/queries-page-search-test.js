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

import hbs from 'htmlbars-inline-precompile';
import { moduleForComponent, test } from 'ember-qunit';
import wait from 'ember-test-helpers/wait';

moduleForComponent('queries-page-search', 'Integration | Component | queries page search', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.render(hbs`{{queries-page-search}}`);
  assert.equal(this.$("input").length, 8);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#queries-page-search}}
      template block text
    {{/queries-page-search}}
  `);
  assert.equal(this.$("input").length, 8);
});

test('tableDefinition test', function(assert) {
  var testQueryID = "query_1",
      testUser = "user",
      testTablesRead = "TablesRead",
      testTablesWritten = "TablesWritten",
      testAppID = "AppID",
      testDagID = "DAGID",
      testQueue = "queue",
      testExecutionMode = "ExecutionMode";

  this.set("tableDefinition", Ember.Object.create({
    queryID: testQueryID,
    user: testUser,
    tablesRead: testTablesRead,
    tablesWritten: testTablesWritten,
    appID: testAppID,
    dagID: testDagID,
    queue: testQueue,
    executionMode: testExecutionMode,
  }));

  this.render(hbs`{{queries-page-search tableDefinition=tableDefinition}}`);

  return wait().then(() => {
    assert.equal(this.$('input').length, 8);
    assert.equal(this.$('input').eq(0).val(), testQueryID);
    assert.equal(this.$('input').eq(1).val(), testUser);
    assert.equal(this.$('input').eq(2).val(), testDagID);
    assert.equal(this.$('input').eq(3).val(), testTablesRead);
    assert.equal(this.$('input').eq(4).val(), testTablesWritten);
    assert.equal(this.$('input').eq(5).val(), testAppID);
    assert.equal(this.$('input').eq(6).val(), testQueue);
    assert.equal(this.$('input').eq(7).val(), testExecutionMode);
  });
});
