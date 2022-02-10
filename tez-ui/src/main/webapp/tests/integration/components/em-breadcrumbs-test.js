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
import { findAll, getRootElement, render } from '@ember/test-helpers';
import { hbs } from 'ember-cli-htmlbars';

module('Integration | Component | em breadcrumbs', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {

    await render(hbs`<EmBreadcrumbs/>`);

    assert.equal(getRootElement().textContent.trim(), '');
  });

  test('Test with one link-to item', async function(assert) {
    var testItems = [{
      routeName: 'foo',
      text: 'fooText'
    }],
    elements;

    this.set('items', testItems);
    await render(hbs`<EmBreadcrumbs @items={{this.items}}/>`);

    elements = findAll('li');

    assert.equal(elements.length, 1);
    assert.equal(elements[0].textContent.trim(), testItems[0].text);
    assert.equal(elements[0].title, testItems[0].text);
    assert.equal(elements[0].style.maxWidth, '100%');
  });
  test('Test with two link-to item', async function(assert) {
    var testItems = [{
      routeName: 'foo',
      text: 'fooText'
    },{
      routeName: 'bar',
      text: 'barText'
    }],
    elements;

    this.set('items', testItems);
    await render(hbs`<EmBreadcrumbs @items={{this.items}}/>`);

    elements = findAll('li');

    assert.equal(elements.length, 2);

    assert.equal(elements[0].textContent.trim(), testItems[0].text);
    assert.equal(elements[0].title, testItems[0].text);
    assert.equal(elements[0].style.maxWidth, '50%');

    assert.equal(elements[1].textContent.trim(), testItems[1].text);
    assert.equal(elements[1].title, testItems[1].text);
    assert.equal(elements[1].style.maxWidth, '50%');
  });

  test('Test with one anchor tag item', async function(assert) {
    var testItems = [{
      href: "foo.bar",
      text: "fooText"
    }],
    elements;

    this.set("items", testItems);
    await render(hbs`<EmBreadcrumbs @items={{this.items}}/>`);

    elements = findAll('li');

    assert.equal(elements.length, 1);
    assert.equal(elements[0].textContent.trim(), testItems[0].text);
    assert.equal(elements[0].title, testItems[0].text);
    assert.equal(elements[0].style.maxWidth, '100%');
  });
});
