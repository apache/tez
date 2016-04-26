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

moduleForComponent('column-selector', 'Integration | Component | column selector', {
  integration: true
});

test('Basic creation test', function(assert) {

  this.set("content", {
    columns: [Ember.Object.create({
      headerTitle: "Test Column"
    })]
  });
  this.render(hbs`{{column-selector content=content}}`);

  assert.equal(this.$(".select-option ").text().trim(), 'Test Column');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#column-selector content=content}}
      template block text
    {{/column-selector}}
  `);

  assert.equal(this.$(".select-option ").text().trim(), 'Test Column');
});

test('visibleColumnIDs test', function(assert) {

  this.setProperties({
    content: {
      visibleColumnIDs: {
        testID: true,
      },
      columns: [Ember.Object.create({
        id: "testID",
        headerTitle: "Test Column"
      })]
    }
  });

  this.render(hbs`{{column-selector content=content}}`);

  assert.equal(this.$(".select-option").text().trim(), 'Test Column');
  assert.equal(this.$(".select-option input")[0].checked, true);
});

test('searchText test', function(assert) {

  this.setProperties({
    searchText: "nothing",
    content: {
      visibleColumnIDs: {
        testID: true,
      },
      columns: [Ember.Object.create({
        id: "testID",
        headerTitle: "Test Column"
      })]
    }
  });

  this.render(hbs`{{column-selector content=content searchText=searchText}}`);

  assert.equal(this.$(".select-option").text().trim(), '');
});
