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

import Ember from 'ember';

moduleForComponent('pagination-ui', 'Integration | Component | pagination ui', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.set("rowCountOptions", {
    rowCountOptions: [1, 2]
  });

  this.render(hbs`{{pagination-ui rowCountOptions=rowCountOptions}}`);

  assert.equal(this.$('select').length, 1);

  assert.equal(this.$('.page-list').length, 1);
  assert.equal(this.$('li').length, 0);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#pagination-ui rowCountOptions=rowCountOptions}}
      template block text
    {{/pagination-ui}}
  `);

  assert.equal(this.$('select').length, 1);
});

test('Page list test', function(assert) {
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

  this.render(hbs`{{pagination-ui tableDefinition=tableDefinition dataProcessor=processor}}`);

  return wait().then(() => {
    assert.equal(this.$('li').length, 4);
    assert.equal(this.$('li').eq(0).text().trim(), "First");
    assert.equal(this.$('li').eq(1).text().trim(), "4");
    assert.equal(this.$('li').eq(2).text().trim(), "5");
    assert.equal(this.$('li').eq(3).text().trim(), "6");
  });
});

test('Page list - moreAvailable false test', function(assert) {
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

  this.render(hbs`{{pagination-ui tableDefinition=tableDefinition dataProcessor=processor}}`);

  return wait().then(() => {
    assert.equal(this.$('li').length, 4);
    assert.equal(this.$('li').eq(1).text().trim(), "3");
    assert.equal(this.$('li').eq(2).text().trim(), "4");
    assert.equal(this.$('li').eq(3).text().trim(), "5");
  });
});

test('Page list - moreAvailable true test', function(assert) {
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

  this.render(hbs`{{pagination-ui tableDefinition=tableDefinition dataProcessor=processor}}`);

  return wait().then(() => {
    assert.equal(this.$('li').length, 4);
    assert.equal(this.$('li').eq(1).text().trim(), "4");
    assert.equal(this.$('li').eq(2).text().trim(), "5");
    assert.equal(this.$('li').eq(3).text().trim(), "6");
  });
});

test('No data test', function(assert) {
  var customRowCount = 2,
      definition = {
        rowCount: customRowCount,
        loadingMore: false,
        moreAvailable: true,

        rowCountOptions: []
      },
      processor;

  Ember.run(function () {
    processor = {
      tableDefinition: definition,
      rows: Ember.A()
    };
  });

  this.set('definition', definition);
  this.set('processor', processor);
  this.render(hbs`{{pagination-ui tableDefinition=definition dataProcessor=processor}}`);

  var paginationItems = this.$('li');
  assert.equal(paginationItems.length, 0);
});