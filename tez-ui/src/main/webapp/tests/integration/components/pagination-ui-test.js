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

import { A } from '@ember/array';
import { run } from '@ember/runloop';
import { render, settled, findAll } from '@ember/test-helpers';
import { setupRenderingTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { hbs } from 'ember-cli-htmlbars';

module('Integration | Component | pagination ui', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {
    this.set("rowCountOptions", {
      rowCountOptions: [1, 2]
    });

    await render(hbs`{{pagination-ui rowCountOptions=rowCountOptions}}`);

    assert.equal(findAll('select').length, 1);

    assert.equal(findAll('.page-list').length, 1);
    assert.equal(findAll('li').length, 0);

    // Template block usage:" + EOL +
    await render(hbs`
      {{#pagination-ui rowCountOptions=rowCountOptions}}
        template block text
      {{/pagination-ui}}
    `);

    assert.equal(findAll('select').length, 1);
  });

  test('Page list test', async function(assert) {
    this.set("tableDefinition", {
      pageNum: 5,
      rowCount: 5,

      loadingMore: false,
      moreAvailable: true,

      rowCountOptions: []
    });
    this.set("processor", {
      totalPages: 10,
      processedRows: {
        length: 10
      }
    });

    await render(hbs`{{pagination-ui tableDefinition=tableDefinition dataProcessor=processor}}`);

    return settled().then(() => {
      let listItems = findAll('li');
      assert.equal(listItems.length, 4);
      assert.dom(listItems[0]).hasText("First");
      assert.dom(listItems[1]).hasText("4");
      assert.dom(listItems[2]).hasText("5");
      assert.dom(listItems[3]).hasText("6");
    });
  });

  test('Page list - moreAvailable false test', async function(assert) {
    this.set("tableDefinition", {
      pageNum: 5,
      rowCount: 5,

      loadingMore: false,
      moreAvailable: false,

      rowCountOptions: []
    });
    this.set("processor", {
      totalPages: 5,
      processedRows: {
        length: 10
      }
    });

    await render(hbs`{{pagination-ui tableDefinition=tableDefinition dataProcessor=processor}}`);

    return settled().then(() => {
      let listItems = findAll('li');
      assert.equal(listItems.length, 4);
      assert.dom(listItems[1]).hasText("3");
      assert.dom(listItems[2]).hasText("4");
      assert.dom(listItems[3]).hasText("5");
    });
  });

  test('Page list - moreAvailable true test', async function(assert) {
    this.set("tableDefinition", {
      pageNum: 5,
      rowCount: 5,

      loadingMore: false,
      moreAvailable: true,

      rowCountOptions: []
    });
    this.set("processor", {
      totalPages: 5,
      processedRows: {
        length: 10
      }
    });

    await render(hbs`{{pagination-ui tableDefinition=tableDefinition dataProcessor=processor}}`);

    return settled().then(() => {
      let listItems = findAll('li');
      assert.equal(listItems.length, 4);
      assert.dom(listItems[1]).hasText("4");
      assert.dom(listItems[2]).hasText("5");
      assert.dom(listItems[3]).hasText("6");
    });
  });

  test('No data test', async function(assert) {
    var customRowCount = 2,
        definition = {
          rowCount: customRowCount,
          loadingMore: false,
          moreAvailable: true,

          rowCountOptions: []
        },
        processor;

    run(function () {
      processor = {
        tableDefinition: definition,
        rows: A()
      };
    });

    this.set('definition', definition);
    this.set('processor', processor);
    await render(hbs`{{pagination-ui tableDefinition=definition dataProcessor=processor}}`);

    var paginationItems = findAll('li');
    assert.equal(paginationItems.length, 0);
  });
});
