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

import DataProcessor from '../../../utils/data-processor';
import TableDefinition from '../../../utils/table-definition';

import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('em-table-pagination-ui', 'Integration | Component | em table pagination ui', {
  integration: true
});

test('Basic rendering test', function(assert) {
  var customRowCount = 25,
      definition = TableDefinition.create({
        rowCount: customRowCount
      }),
      processor;

  Ember.run(function () {
    processor = DataProcessor.create({
      tableDefinition: definition,
      rows: Ember.A([Ember.Object.create()])
    });
  });

  this.set('definition', definition);
  this.set('processor', processor);
  this.render(hbs`{{em-table-pagination-ui tableDefinition=definition dataProcessor=processor}}`);

  var paginationItems = this.$('li');
  assert.equal(paginationItems.length, 1);
  assert.equal($(paginationItems[0]).text().trim(), "1");

  var rowSelection = this.$('select')[0];
  assert.ok(rowSelection);
  assert.equal($(rowSelection).val(), customRowCount);
});

test('No data test', function(assert) {
  var customRowCount = 2,
      definition = TableDefinition.create({
        rowCount: customRowCount
      }),
      processor;

  Ember.run(function () {
    processor = DataProcessor.create({
      tableDefinition: definition,
      rows: Ember.A()
    });
  });

  this.set('definition', definition);
  this.set('processor', processor);
  this.render(hbs`{{em-table-pagination-ui tableDefinition=definition dataProcessor=processor}}`);

  var paginationItems = this.$('li');
  assert.equal(paginationItems.length, 0);
});

test('Multiple page test; without first & last', function(assert) {
  var customRowCount = 2,
      definition = TableDefinition.create({
        rowCount: customRowCount
      }),
      processor;

  Ember.run(function () {
    processor = DataProcessor.create({
      tableDefinition: definition,
      rows: Ember.A([Ember.Object.create(), Ember.Object.create(), Ember.Object.create()])
    });
  });

  this.set('definition', definition);
  this.set('processor', processor);
  this.render(hbs`{{em-table-pagination-ui tableDefinition=definition dataProcessor=processor}}`);

  var paginationItems = this.$('li');
  assert.equal(paginationItems.length, 2);
  assert.equal($(paginationItems[0]).text().trim(), "1");
  assert.equal($(paginationItems[1]).text().trim(), "2");
});

test('Display last test', function(assert) {
  var customRowCount = 5,
      definition = TableDefinition.create({
        rowCount: customRowCount
      }),
      processor,
      rows = [];

  for(var i = 0; i < 100; i++) {
    rows.push(Ember.Object.create());
  }

  Ember.run(function () {
    processor = DataProcessor.create({
      tableDefinition: definition,
      rows: Ember.A(rows)
    });
  });

  this.set('definition', definition);
  this.set('processor', processor);
  this.render(hbs`{{em-table-pagination-ui tableDefinition=definition dataProcessor=processor}}`);

  var paginationItems = this.$('li');
  assert.equal(paginationItems.length, 6);
  assert.equal($(paginationItems[0]).text().trim(), "1");
  assert.equal($(paginationItems[1]).text().trim(), "2");
  assert.equal($(paginationItems[2]).text().trim(), "3");
  assert.equal($(paginationItems[3]).text().trim(), "4");
  assert.equal($(paginationItems[4]).text().trim(), "5");
  assert.equal($(paginationItems[5]).text().trim(), "Last - 20");
});

test('Display first test', function(assert) {
  var customRowCount = 5,
      definition = TableDefinition.create({
        pageNum: 20,
        rowCount: customRowCount
      }),
      processor,
      rows = [];

  for(var i = 0; i < 100; i++) {
    rows.push(Ember.Object.create());
  }

  Ember.run(function () {
    processor = DataProcessor.create({
      tableDefinition: definition,
      rows: Ember.A(rows)
    });
  });

  this.set('definition', definition);
  this.set('processor', processor);
  this.render(hbs`{{em-table-pagination-ui tableDefinition=definition dataProcessor=processor}}`);

  var paginationItems = this.$('li');
  assert.equal(paginationItems.length, 6);
  assert.equal($(paginationItems[0]).text().trim(), "First");
  assert.equal($(paginationItems[1]).text().trim(), "16");
  assert.equal($(paginationItems[2]).text().trim(), "17");
  assert.equal($(paginationItems[3]).text().trim(), "18");
  assert.equal($(paginationItems[4]).text().trim(), "19");
  assert.equal($(paginationItems[5]).text().trim(), "20");
});

test('Display first & last test', function(assert) {
  var customRowCount = 5,
      definition = TableDefinition.create({
        pageNum: 10,
        rowCount: customRowCount
      }),
      processor,
      rows = [];

  for(var i = 0; i < 100; i++) {
    rows.push(Ember.Object.create());
  }

  Ember.run(function () {
    processor = DataProcessor.create({
      tableDefinition: definition,
      rows: Ember.A(rows)
    });
  });

  this.set('definition', definition);
  this.set('processor', processor);
  this.render(hbs`{{em-table-pagination-ui tableDefinition=definition dataProcessor=processor}}`);

  var paginationItems = this.$('li');
  assert.equal(paginationItems.length, 7);
  assert.equal($(paginationItems[0]).text().trim(), "First");
  assert.equal($(paginationItems[1]).text().trim(), "8");
  assert.equal($(paginationItems[2]).text().trim(), "9");
  assert.equal($(paginationItems[3]).text().trim(), "10");
  assert.equal($(paginationItems[4]).text().trim(), "11");
  assert.equal($(paginationItems[5]).text().trim(), "12");
  assert.equal($(paginationItems[6]).text().trim(), "Last - 20");
});
