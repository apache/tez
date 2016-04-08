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

import Process from 'tez-ui/utils/process';

moduleForComponent('em-swimlane-vertex-name', 'Integration | Component | em swimlane vertex name', {
  integration: true
});

test('Basic creation test', function(assert) {

  this.render(hbs`{{em-swimlane-vertex-name}}`);
  assert.equal(this.$().text().trim(), 'Not Available!');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-swimlane-vertex-name}}
      template block text
    {{/em-swimlane-vertex-name}}
  `);
  assert.equal(this.$().text().trim(), 'Not Available!');
});

test('Name test', function(assert) {
  this.set("process", Process.create({
    name: "TestName"
  }));

  this.render(hbs`{{em-swimlane-vertex-name process=process}}`);
  return wait().then(() => {
    assert.equal(this.$(".name-text").text().trim(), 'TestName');
  });
});

test('Progress test', function(assert) {
  this.set("process", Process.create({
    vertex: {
      finalStatus: "RUNNING",
      progress: 0.5
    }
  }));

  this.render(hbs`{{em-swimlane-vertex-name process=process}}`);
  return wait().then(() => {
    assert.equal(this.$().text().trim().substr(0, 3), '50%');
  });
});

test('finalStatus test', function(assert) {
  this.set("process", Process.create({
    vertex: {
      finalStatus: "STAT"
    }
  }));

  this.render(hbs`{{em-swimlane-vertex-name process=process}}`);
  return wait().then(() => {
    assert.equal(this.$().text().trim(), 'STAT');
  });
});
