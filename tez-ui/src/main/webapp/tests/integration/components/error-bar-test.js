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

import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

import wait from 'ember-test-helpers/wait';

moduleForComponent('error-bar', 'Integration | Component | error bar', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.render(hbs`{{error-bar}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#error-bar}}
      template block text
    {{/error-bar}}
  `);

  assert.equal(this.$().text().trim(), '');
});

test('Plain Object test', function(assert) {
  Function.prototype.bind = function () {};

  this.set("error", {});
  this.render(hbs`{{error-bar error=error}}`);

  return wait().then(() => {
    assert.equal(this.$().text().trim(), 'Error');
  });
});

test('Message test', function(assert) {
  var testMessage = "Test Message";

  Function.prototype.bind = function () {};

  this.set("error", {
    message: testMessage
  });
  this.render(hbs`{{error-bar error=error}}`);

  return wait().then(() => {
    assert.equal(this.$().text().trim(), testMessage);
  });
});

test('details test', function(assert) {
  var testMessage = "Test Message",
      testDetails = "details";

  Function.prototype.bind = function () {};

  this.set("error", {
    message: testMessage,
    details: testDetails
  });
  this.render(hbs`{{error-bar error=error}}`);

  return wait().then(() => {
    assert.equal(this.$(".message").text().trim(), testMessage);
    assert.equal(this.$(".details p").text().trim(), testDetails);
  });
});

test('requestInfo test', function(assert) {
  var testMessage = "Test Message",
      testInfo = "info";

  Function.prototype.bind = function () {};

  this.set("error", {
    message: testMessage,
    requestInfo: testInfo
  });
  this.render(hbs`{{error-bar error=error}}`);

  return wait().then(() => {
    assert.equal(this.$(".message").text().trim(), testMessage);
    assert.equal(this.$(".details p").text().trim(), testInfo);
  });
});

test('stack test', function(assert) {
  var testMessage = "Test Message",
      testStack = "stack";

  Function.prototype.bind = function () {};

  this.set("error", {
    message: testMessage,
    stack: testStack
  });
  this.render(hbs`{{error-bar error=error}}`);

  return wait().then(() => {
    assert.equal(this.$(".message").text().trim(), testMessage);
    assert.equal(this.$(".details p").text().trim(), testStack);
  });
});
