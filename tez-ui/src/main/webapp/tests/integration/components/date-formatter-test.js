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

moduleForComponent('date-formatter', 'Integration | Component | em table date-formatter cell', {
  integration: true
});

test('Basic creation test', function(assert) {

  this.render(hbs`{{date-formatter}}`);

  assert.equal(this.$().text().trim(), 'Not Available!');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#date-formatter}}
      template block text
    {{/date-formatter}}
  `);

  assert.equal(this.$().text().trim(), 'Not Available!');
});

test('Negative value test', function(assert) {
  this.render(hbs`{{date-formatter -1}}`);
  assert.equal(this.$().text().trim(), 'Not Available!');

  this.render(hbs`{{date-formatter -99}}`);
  assert.equal(this.$().text().trim(), 'Not Available!');

  this.render(hbs`{{date-formatter 0}}`);
  assert.equal(this.$().text().trim(), 'Not Available!');
});