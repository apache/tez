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

import Processor from 'tez-ui/utils/processor';

moduleForComponent('em-swimlane-ruler', 'Integration | Component | em swimlane ruler', {
  integration: true
});

test('Basic creation test', function(assert) {

  this.render(hbs`{{em-swimlane-ruler}}`);
  assert.equal(this.$().text().trim(), 'Milliseconds');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-swimlane-ruler}}
      template block text
    {{/em-swimlane-ruler}}
  `);
  assert.equal(this.$().text().trim(), 'Milliseconds');
});

test('Mark test', function(assert) {
  this.set("processor", Processor.create({
    startTime: 0,
    endTime: 1000 * 20,
  }));

  this.render(hbs`{{em-swimlane-ruler processor=processor zoom=100}}`);

  return wait().then(() => {
    assert.equal(this.$(".unit-text").text().trim(), 'Seconds');
    assert.equal(this.$(".ruler-mark").length, 11);
  });
});

test('Mark zoom test', function(assert) {
  this.set("processor", Processor.create({
    startTime: 0,
    endTime: 1000 * 20,
  }));

  this.render(hbs`{{em-swimlane-ruler processor=processor zoom=500}}`);

  return wait().then(() => {
    assert.equal(this.$(".unit-text").text().trim(), 'Milliseconds');
    assert.equal(this.$(".ruler-mark").length, 55);
  });
});
