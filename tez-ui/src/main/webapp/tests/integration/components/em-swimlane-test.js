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

import { setupRenderingTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { find, findAll, render } from '@ember/test-helpers';
import { hbs } from 'ember-cli-htmlbars';

import Process from 'tez-ui/utils/process';

module('Integration | Component | em swimlane', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {
    var testName1 = "TestName1",
        testName2 = "TestName2";

    this.set("processes", [Process.create({
      name: testName1
    }), Process.create({
      name: testName2
    })]);

    await render(hbs`<EmSwimlane @processes={{this.processes}}/>`);

    assert.equal(this.element.textContent.trim().indexOf(testName1), 0);
    assert.notEqual(this.element.textContent.trim().indexOf(testName2), -1);

    // Template block usage:" + EOL +
    await render(hbs`
      <EmSwimlane @processes={{this.processes}}>
        template block text
      </EmSwimlane>
    `);

    assert.equal(this.element.textContent.trim().indexOf(testName1), 0);
    assert.notEqual(this.element.textContent.trim().indexOf(testName2), -1);
  });

  test('Normalization (Blocker based sorting) test - On a graph', async function(assert) {
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

    await render(hbs`<EmSwimlane @processes={{this.processes}}/>`);

    let names = findAll(".em-swimlane-process-name");

    assert.equal(names.length, 5);
    assert.dom(names[0]).hasText(p1.name);
    assert.dom(names[1]).hasText(p4.name);
    assert.dom(names[2]).hasText(p2.name);
    assert.dom(names[3]).hasText(p3.name);
    assert.dom(names[4]).hasText(p5.name);
  });

  test('Zoom test', async function(assert) {
    this.set("processes", [Process.create()]);

    await render(hbs`<EmSwimlane @processes={{this.processes}} @zoom={{500}}/>`);
    assert.equal(find(".zoom-panel").getAttribute("style").trim(), "width: 500%;");
  });
});
