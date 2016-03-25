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

moduleForComponent('em-tooltip', 'Integration | Component | em tooltip', {
  integration: true
});

test('Basic creation test', function(assert) {

  this.render(hbs`{{em-tooltip}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-tooltip}}
      template block text
    {{/em-tooltip}}
  `);

  assert.equal(this.$().text().trim(), '');
});

test('Title test', function(assert) {
  this.set("title", "TestTitle");
  this.render(hbs`{{em-tooltip title=title}}`);

  assert.equal(this.$().text().trim(), 'TestTitle');
});

test('Description test', function(assert) {
  this.set("desc", "TestDesc");
  this.render(hbs`{{em-tooltip description=desc}}`);

  assert.equal(this.$().text().trim(), 'TestDesc');
});

test('Properties test', function(assert) {
  this.set("properties", [{
    name: "p1", value: "v1"
  }, {
    name: "p2", value: "v2"
  }]);
  this.render(hbs`{{em-tooltip properties=properties}}`);

  assert.equal(this.$("tr").length, 2);
});

test('Contents test', function(assert) {
  this.set("contents", [{
    title: "p1",
    properties: [{}, {}]
  }, {
    title: "p2",
    properties: [{}, {}, {}]
  }]);

  this.render(hbs`{{em-tooltip contents=contents}}`);

  assert.equal(this.$(".bubble").length, 2);
  assert.equal(this.$("tr").length, 2 + 3);
});
