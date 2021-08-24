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

import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('em-breadcrumbs', 'Integration | Component | em breadcrumbs', {
  integration: true
});

test('Basic creation test', function(assert) {

  // Set any properties with this.set('myProperty', 'value');
  // Handle any actions with this.on('myAction', function(val) { ... });" + EOL + EOL +

  this.render(hbs`{{em-breadcrumbs}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-breadcrumbs}}
      template block text
    {{/em-breadcrumbs}}
  `);

  assert.equal(this.$().text().trim(), '');
});

test('Test with one link-to item', function(assert) {
  var testItems = [{
    routeName: "foo",
    text: "fooText"
  }],
  elements;

  this.set("items", testItems);
  this.render(hbs`{{em-breadcrumbs items=items}}`);

  elements = this.$("li");

  assert.equal(elements.length, 1);
  assert.equal(Ember.$(elements[0]).text().trim(), testItems[0].text);
  assert.equal(elements[0].title, testItems[0].text);
  assert.equal(elements[0].style.maxWidth, "100%");
});

test('Test with two link-to item', function(assert) {
  var testItems = [{
    routeName: "foo",
    text: "fooText"
  },{
    routeName: "bar",
    text: "barText"
  }],
  elements;

  this.set("items", testItems);
  this.render(hbs`{{em-breadcrumbs items=items}}`);

  elements = this.$("li");

  assert.equal(elements.length, 2);

  assert.equal(Ember.$(elements[0]).text().trim(), testItems[0].text);
  assert.equal(elements[0].title, testItems[0].text);
  assert.equal(elements[0].style.maxWidth, "50%");

  assert.equal(Ember.$(elements[1]).text().trim(), testItems[1].text);
  assert.equal(elements[1].title, testItems[1].text);
  assert.equal(elements[1].style.maxWidth, "50%");
});

test('Test with one anchor tag item', function(assert) {
  var testItems = [{
    href: "foo.bar",
    text: "fooText"
  }],
  elements;

  this.set("items", testItems);
  this.render(hbs`{{em-breadcrumbs items=items}}`);

  elements = this.$("li");

  assert.equal(elements.length, 1);
  assert.equal(Ember.$(elements[0]).text().trim(), testItems[0].text);
  assert.equal(elements[0].title, testItems[0].text);
  assert.equal(elements[0].style.maxWidth, "100%");
});
