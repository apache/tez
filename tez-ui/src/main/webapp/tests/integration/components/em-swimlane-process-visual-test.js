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
import Processor from 'tez-ui/utils/processor';

module('Integration | Component | em swimlane process visual', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {
    this.set("process", Process.create());
    this.set("processor", Processor.create());

    await render(hbs`<EmSwimlaneProcessVisual @process={{this.process}} @processor={{this.processor}}/>`);

    assert.ok(find(".base-line"));
    assert.ok(find(".process-line"));

    // Template block usage:" + EOL +
    await render(hbs`
      <EmSwimlaneProcessVisual @process={{this.process}} @processor={{this.processor}}>
        template block text
      </EmSwimlaneProcessVisual>}
    `);

    assert.ok(find(".base-line"));
    assert.ok(find(".process-line"));
  });

  test('Events test', async function(assert) {
    this.set("process", Process.create({
      events: [{
        name: "event1",
        time: 5
      }, {
        name: "event2",
        time: 7
      }]
    }));
    this.set("processor", Processor.create({
      startTime: 0,
      endTime: 10
    }));

    await render(hbs`<EmSwimlaneProcessVisual @process={{this.process}} @processor={{this.processor}} />`);

    var events = findAll(".em-swimlane-event");

    assert.equal(events.length, 2);
    assert.equal(events[0].style.left, '50%');
    assert.equal(events[1].style.left, '70%');
    let processLine = find('.process-line');
    assert.ok(processLine);
    assert.equal(processLine.style.left, '50%');
    assert.equal(processLine.style.right, '30%');
  });
});
