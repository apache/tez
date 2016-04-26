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

import Process from 'tez-ui/utils/process';

moduleForComponent('em-swimlane', 'Integration | Component | em swimlane', {
  integration: true
});

test('Basic creation test', function(assert) {
  var testName1 = "TestName1",
      testName2 = "TestName2";

  this.set("processes", [Process.create({
    name: testName1
  }), Process.create({
    name: testName2
  })]);

  this.render(hbs`{{em-swimlane processes=processes}}`);

  assert.equal(this.$().text().trim().indexOf(testName1), 0);
  assert.notEqual(this.$().text().trim().indexOf(testName2), -1);

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-swimlane processes=processes}}
      template block text
    {{/em-swimlane}}
  `);

  assert.equal(this.$().text().trim().indexOf(testName1), 0);
  assert.notEqual(this.$().text().trim().indexOf(testName2), -1);
});

test('Normalization (Blocker based sorting) test - On a graph', function(assert) {
  var p1 = Process.create({
    name: "P1"
  }),
  p2 = Process.create({
    name: "P2"
  }),
  p3 = Process.create({
    name: "P3",
    blockers: [p1, p2]
  }),
  p4 = Process.create({
    name: "P4",
    blockers: [p1]
  }),
  p5 = Process.create({
    name: "P5",
    blockers: [p3, p4]
  });

  this.set("processes", [p5, p4, p3, p2, p1]);

  this.render(hbs`{{em-swimlane processes=processes}}`);

  let names = this.$(".em-swimlane-process-name");

  assert.equal(names.length, 5);
  assert.equal(names.eq(0).text().trim(), p1.name);
  assert.equal(names.eq(1).text().trim(), p4.name);
  assert.equal(names.eq(2).text().trim(), p2.name);
  assert.equal(names.eq(3).text().trim(), p3.name);
  assert.equal(names.eq(4).text().trim(), p5.name);
});

test('Zoom test', function(assert) {
  this.set("processes", [Process.create()]);

  this.render(hbs`{{em-swimlane processes=processes zoom=500}}`);
  assert.equal(this.$(".zoom-panel").attr("style").trim(), "width: 500%;");
});
