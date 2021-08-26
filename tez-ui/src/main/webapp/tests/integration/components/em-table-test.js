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

import TableDefinition from '../../../utils/table-definition';
import ColumnDefinition from '../../../utils/column-definition';

moduleForComponent('em-table', 'Integration | Component | em table', {
  integration: true
});

test('Basic rendering test', function(assert) {
  this.render(hbs`{{em-table}}`);

  assert.equal(this.$('.table-message').text().trim(), 'No columns available!');
});

test('Records missing test', function(assert) {
  var definition = TableDefinition.create({
    recordType: "vertex"
  });

  this.set("columns", [ColumnDefinition.fillerColumn]);

  this.render(hbs`{{em-table columns=columns}}`);
  assert.equal(this.$('.table-message').text().trim(), 'No records available!');

  this.set("definition", definition);
  this.render(hbs`{{em-table columns=columns definition=definition}}`);
  assert.equal(this.$('.table-message').text().trim(), 'No vertices available!');
});
