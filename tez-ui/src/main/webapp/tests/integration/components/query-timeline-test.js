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

moduleForComponent('query-timeline', 'Integration | Component | query timeline', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.set("perf", {});
  this.render(hbs`{{query-timeline perf=perf}}`);

  assert.equal(this.$().find(".bar").length, 10 + 4);

  this.render(hbs`
    {{#query-timeline perf=perf}}
      template block text
    {{/query-timeline}}
  `);

  assert.equal(this.$().find(".bar").length, 10 + 4);
});

test('alignBars test', function(assert) {
  var total = 10 + 20 + 30 + 40 + 50 + 60 + 70 + 80 + 90 + 100;
  var bars;

  this.set("perf", {
    "compile": 10,
    "parse": 20,
    "semanticAnalyze": 30,
    "TezBuildDag": 40,

    "TezSubmitDag": 50,
    "TezSubmitToRunningDag": 60,

    "TezRunDag": 70,

    "PostATSHook": 80,
    "RemoveTempOrDuplicateFiles": 90,
    "RenameOrMoveFiles": 100,
  });
  this.render(hbs`{{query-timeline perf=perf}}`);

  function assertWidth(domElement, factor) {
    var elementWidth = (parseFloat(domElement.style.width) / 100).toFixed(4),
        expectedWidth = (factor / total).toFixed(4);

    assert.equal(elementWidth, expectedWidth, `Unexpected value for factor ${factor}`);
  }

  bars = this.$().find(".groups").find(".bar");
  assert.equal(bars.length, 4);
  assertWidth(bars[0], 10 + 20 + 30 + 40);
  assertWidth(bars[1], 50 + 60);
  assertWidth(bars[2], 70);
  assertWidth(bars[3], 80 + 90 + 100);

  bars = this.$().find(".sub-groups").find(".bar");
  assert.equal(bars.length, 10);
  assertWidth(bars[0], 10);
  assertWidth(bars[1], 20);
  assertWidth(bars[2], 30);
  assertWidth(bars[3], 40);
  assertWidth(bars[4], 50);
  assertWidth(bars[5], 60);
  assertWidth(bars[6], 70);
  assertWidth(bars[7], 80);
  assertWidth(bars[8], 90);
  assertWidth(bars[9], 100);
});

test('alignBars - without RenameOrMoveFiles test', function(assert) {
  var total = 10 + 20 + 30 + 40 + 50 + 60 + 70 + 80 + 90;
  var bars;

  this.set("perf", {
    "compile": 10,
    "parse": 20,
    "semanticAnalyze": 30,
    "TezBuildDag": 40,

    "TezSubmitDag": 50,
    "TezSubmitToRunningDag": 60,

    "TezRunDag": 70,

    "PostATSHook": 80,
    "RemoveTempOrDuplicateFiles": 90,
  });
  this.render(hbs`{{query-timeline perf=perf}}`);

  function assertWidth(domElement, factor) {
    var elementWidth = (parseFloat(domElement.style.width) / 100).toFixed(4),
        expectedWidth = (factor / total).toFixed(4);

    assert.equal(elementWidth, expectedWidth, `Unexpected value for factor ${factor}`);
  }

  bars = this.$().find(".groups").find(".bar");
  assert.equal(bars.length, 4);
  assertWidth(bars[0], 10 + 20 + 30 + 40);
  assertWidth(bars[1], 50 + 60);
  assertWidth(bars[2], 70);
  assertWidth(bars[3], 80 + 90);

  bars = this.$().find(".sub-groups").find(".bar");
  assert.equal(bars.length, 10);
  assertWidth(bars[0], 10);
  assertWidth(bars[1], 20);
  assertWidth(bars[2], 30);
  assertWidth(bars[3], 40);
  assertWidth(bars[4], 50);
  assertWidth(bars[5], 60);
  assertWidth(bars[6], 70);
  assertWidth(bars[7], 80);
  assertWidth(bars[8], 90);
  assertWidth(bars[9], 0);
});