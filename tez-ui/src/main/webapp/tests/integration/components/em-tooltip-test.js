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
import { render, findAll } from '@ember/test-helpers';
import { hbs } from 'ember-cli-htmlbars';

module('Integration | Component | em tooltip', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {

    await render(hbs`{{em-tooltip}}`);

    assert.equal(this.element.textContent.trim(), '');

    // Template block usage:" + EOL +
    await render(hbs`
      {{#em-tooltip}}
        template block text
      {{/em-tooltip}}
    `);

    assert.equal(this.element.textContent.trim(), '');
  });

  test('Title test', async function(assert) {
    this.set("title", "TestTitle");
    await render(hbs`{{em-tooltip title=title}}`);

    assert.equal(this.element.textContent.trim(), 'TestTitle');
  });

  test('Description test', async function(assert) {
    this.set("desc", "TestDesc");
    await render(hbs`{{em-tooltip description=desc}}`);

    assert.equal(this.element.textContent.trim(), 'TestDesc');
  });

  test('Properties test', async function(assert) {
    this.set("properties", [{
      name: "p1", value: "v1"
    }, {
      name: "p2", value: "v2"
    }]);
    await render(hbs`{{em-tooltip properties=properties}}`);

    assert.equal(findAll("tr").length, 2);
  });

  test('Contents test', async function(assert) {
    this.set("contents", [{
      title: "p1",
      properties: [{}, {}]
    }, {
      title: "p2",
      properties: [{}, {}, {}]
    }]);

    await render(hbs`{{em-tooltip contents=contents}}`);

    assert.equal(findAll(".bubble").length, 2);
    assert.equal(findAll("tr").length, 2 + 3);
  });
});
