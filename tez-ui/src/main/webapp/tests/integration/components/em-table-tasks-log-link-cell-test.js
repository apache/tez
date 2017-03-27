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

moduleForComponent('em-table-tasks-log-link-cell', 'Integration | Component | em table tasks log link cell', {
  integration: true
});

test('Basic render test', function(assert) {
  this.render(hbs`{{em-table-tasks-log-link-cell}}`);

  assert.equal(this.$().text().trim(), 'Not Available!');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-table-tasks-log-link-cell}}
      template block text
    {{/em-table-tasks-log-link-cell}}
  `);

  assert.equal(this.$().text().trim(), 'Not Available!');
});

test('Test with content', function(assert) {
  let attemptID = "attempt_1";

  this.set("content", attemptID);
  this.render(hbs`{{em-table-tasks-log-link-cell content=content}}`);

  let tags = this.$().find("a");
  assert.equal(tags.length, 2);
  assert.equal(Ember.$(tags[0]).text().trim(), 'View');
  assert.equal(Ember.$(tags[1]).text().trim(), 'Download');
});
