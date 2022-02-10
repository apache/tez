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

import { setupRenderingTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { render, settled, find } from '@ember/test-helpers';
import { hbs } from 'ember-cli-htmlbars';

module('Integration | Component | error bar', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {
    await render(hbs`<ErrorBar/>`);

    assert.dom(this.element).hasText('');

    // Template block usage:" + EOL +
    await render(hbs`
      <ErrorBar>
        template block text
      </ErrorBar>
    `);

    assert.dom(this.element).hasText('');
  });

  test('Plain Object test', async function(assert) {

    this.set("error", {});
    await render(hbs`<ErrorBar @error={{this.error}}/>`);

    return settled().then(() => {
      assert.dom(this.element).hasText('Error');
    });
  });

  test('Message test', async function(assert) {
    var testMessage = "Test Message";


    this.set("error", {
      message: testMessage
    });
    await render(hbs`<ErrorBar @error={{this.error}}/>`);

    return settled().then(() => {
      assert.equal(this.element.textContent.trim(), testMessage);
    });
  });

  test('details test', async function(assert) {
    var testMessage = "Test Message",
        testDetails = "details";


    this.set("error", {
      message: testMessage,
      details: testDetails
    });
    await render(hbs`<ErrorBar @error={{this.error}}/>`);

    return settled().then(() => {
      assert.equal(find(".message").textContent.trim(), testMessage);
      assert.equal(find(".details p").textContent.trim(), testDetails);
    });
  });

  test('requestInfo test', async function(assert) {
    var testMessage = "Test Message",
        testInfo = "info";


    this.set("error", {
      message: testMessage,
      requestInfo: testInfo
    });
    await render(hbs`<ErrorBar @error={{this.error}}/>`);

    return settled().then(() => {
      assert.equal(find(".message").textContent.trim(), testMessage);
      assert.equal(find(".details p").textContent.trim(), testInfo);
    });
  });

  test('stack test', async function(assert) {
    var testMessage = "Test Message",
        testStack = "stack";


    this.set("error", {
      message: testMessage,
      stack: testStack
    });
    await render(hbs`<ErrorBar @error={{this.error}}/>`);

    return settled().then(() => {
      assert.equal(find(".message").textContent.trim(), testMessage);
      assert.equal(find(".details p").textContent.trim(), testStack);
    });
  });
});
