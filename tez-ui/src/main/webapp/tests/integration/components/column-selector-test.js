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
import { setupRenderingTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { render, find } from '@ember/test-helpers';
import { hbs } from 'ember-cli-htmlbars';

module('Integration | Component | column selector', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {

    this.set("content", {
      columns: [EmberObject.create({
        headerTitle: "Test Column"
      })]
    });
    await render(hbs`<ColumnSelector @content={{this.content}}/>`);

    assert.dom('.select-option').hasText('Test Column');

    // Template block usage:" + EOL +
    await render(hbs`
      <ColumnSelector @content={{this.content}}>
        template block text
      </ColumnSelector>
    `);

    assert.dom('.select-option').hasText('Test Column');
  });

  test('visibleColumnIDs test', async function(assert) {

    this.setProperties({
      content: {
        visibleColumnIDs: {
          testID: true,
        },
        columns: [EmberObject.create({
          id: "testID",
          headerTitle: "Test Column"
        })]
      }
    });

    await render(hbs`<ColumnSelector @content={{this.content}}/>`);

    assert.dom('.select-option').hasText('Test Column');
    assert.true(find(".select-option input").checked);
  });

  test('searchText no results test', async function(assert) {

    this.setProperties({
      searchText: "nothing",
      content: {
        visibleColumnIDs: {
          testID: true,
        },
        columns: [EmberObject.create({
          id: "testID",
          headerTitle: "Test Column"
        })]
      }
    });

    await render(hbs`<ColumnSelector @content={{this.content}} @searchText={{this.searchText}}/>`);

    assert.equal(find(".select-option"), null);
  });

  test('case-insensitive searchText test', async function(assert) {

    this.setProperties({
      searchText: "test",
      content: {
        visibleColumnIDs: {
          testID: true,
        },
        columns: [EmberObject.create({
          id: "testID",
          headerTitle: "Test Column"
        })]
      }
    });

    await render(hbs`<ColumnSelector @content={{this.content}} @searchText={{this.searchText}}/>`);

    assert.dom('.select-option').hasText('Test Column');
  });
});
