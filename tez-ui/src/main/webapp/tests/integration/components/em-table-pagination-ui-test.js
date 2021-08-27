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
import EmberObject from '@ember/object';
import { run } from '@ember/runloop';
import { find, findAll, render } from '@ember/test-helpers';
import { setupRenderingTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { hbs } from 'ember-cli-htmlbars';

import DataProcessor from '../../../utils/data-processor';
import TableDefinition from '../../../utils/table-definition';

module('Integration | Component | em table pagination ui', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic rendering test', async function(assert) {
    var customRowCount = 25,
        definition = TableDefinition.create({
          rowCount: customRowCount
        }),
        processor;

    run(function () {
      processor = DataProcessor.create({
        tableDefinition: definition,
        rows: A([EmberObject.create()])
      });
    });

    this.set('tableDefinition', definition);
    this.set('dataProcessor', processor);
    await render(hbs`<EmTablePaginationUi @tableDefinition={{this.tableDefinition}} @dataProcessor={{this.dataProcessor}}/>`);

    var paginationItems = findAll('li');
    assert.equal(paginationItems.length, 1);
    assert.dom(paginationItems[0]).hasText('1');

    var rowSelection = find('select');
    assert.ok(rowSelection);
    assert.equal(rowSelection.value, customRowCount);
  });

  test('No data test', async function(assert) {
    var customRowCount = 2,
        definition = TableDefinition.create({
          rowCount: customRowCount
        }),
        processor;

    run(function () {
      processor = DataProcessor.create({
        tableDefinition: definition,
        rows: A()
      });
    });

    this.set('tableDefinition', definition);
    this.set('dataProcessor', processor);
    await render(hbs`<EmTablePaginationUi @tableDefinition={{this.tableDefinition}} @dataProcessor={{this.dataProcessor}}/>`);

    var paginationItems = findAll('li');
    assert.equal(paginationItems.length, 0);
  });

  test('Multiple page test; without first & last', async function(assert) {
    var customRowCount = 2,
        definition = TableDefinition.create({
          rowCount: customRowCount
        }),
        processor;

    run(function () {
      processor = DataProcessor.create({
        tableDefinition: definition,
        rows: A([EmberObject.create(), EmberObject.create(), EmberObject.create()])
      });
    });

    this.set('tableDefinition', definition);
    this.set('dataProcessor', processor);
    await render(hbs`<EmTablePaginationUi @tableDefinition={{this.tableDefinition}} @dataProcessor={{this.dataProcessor}}/>`);

    var paginationItems = findAll('li');
    assert.equal(paginationItems.length, 2);
    assert.dom(paginationItems[0]).hasText('1');
    assert.dom(paginationItems[1]).hasText('2');
  });

  test('Display last test', async function(assert) {
    var customRowCount = 5,
        definition = TableDefinition.create({
          rowCount: customRowCount
        }),
        processor,
        rows = [];

    for(var i = 0; i < 100; i++) {
      rows.push(EmberObject.create());
    }

    run(function () {
      processor = DataProcessor.create({
        tableDefinition: definition,
        rows: A(rows)
      });
    });

    this.set('tableDefinition', definition);
    this.set('dataProcessor', processor);
    await render(hbs`<EmTablePaginationUi @tableDefinition={{this.tableDefinition}} @dataProcessor={{this.dataProcessor}}/>`);

    var paginationItems = findAll('li');
    assert.equal(paginationItems.length, 6);
    assert.dom(paginationItems[0]).hasText('1');
    assert.dom(paginationItems[1]).hasText('2');
    assert.dom(paginationItems[2]).hasText('3');
    assert.dom(paginationItems[3]).hasText('4');
    assert.dom(paginationItems[4]).hasText('5');
    assert.dom(paginationItems[5]).hasText('Last - 20');
  });

  test('Display first test', async function(assert) {
    var customRowCount = 5,
        definition = TableDefinition.create({
          pageNum: 20,
          rowCount: customRowCount
        }),
        processor,
        rows = [];

    for(var i = 0; i < 100; i++) {
      rows.push(EmberObject.create());
    }

    run(function () {
      processor = DataProcessor.create({
        tableDefinition: definition,
        rows: A(rows)
      });
    });

    this.set('tableDefinition', definition);
    this.set('dataProcessor', processor);
    await render(hbs`<EmTablePaginationUi @tableDefinition={{this.tableDefinition}} @dataProcessor={{this.dataProcessor}}/>`);

    var paginationItems = findAll('li');
    assert.equal(paginationItems.length, 6);
    assert.dom(paginationItems[0]).hasText('First');
    assert.dom(paginationItems[1]).hasText('16');
    assert.dom(paginationItems[2]).hasText('17');
    assert.dom(paginationItems[3]).hasText('18');
    assert.dom(paginationItems[4]).hasText('19');
    assert.dom(paginationItems[5]).hasText('20');
  });

  test('Display first & last test', async function(assert) {
    var customRowCount = 5,
        definition = TableDefinition.create({
          pageNum: 10,
          rowCount: customRowCount
        }),
        processor,
        rows = [];

    for(var i = 0; i < 100; i++) {
      rows.push(EmberObject.create());
    }

    run(function () {
      processor = DataProcessor.create({
        tableDefinition: definition,
        rows: A(rows)
      });
    });

    this.set('tableDefinition', definition);
    this.set('dataProcessor', processor);
    await render(hbs`<EmTablePaginationUi @tableDefinition={{this.tableDefinition}} @dataProcessor={{this.dataProcessor}}/>`);

    var paginationItems = findAll('li');
    assert.equal(paginationItems.length, 7);
    assert.dom(paginationItems[0]).hasText('First');
    assert.dom(paginationItems[1]).hasText('8');
    assert.dom(paginationItems[2]).hasText('9');
    assert.dom(paginationItems[3]).hasText('10');
    assert.dom(paginationItems[4]).hasText('11');
    assert.dom(paginationItems[5]).hasText('12');
    assert.dom(paginationItems[6]).hasText('Last - 20');
  });
});
