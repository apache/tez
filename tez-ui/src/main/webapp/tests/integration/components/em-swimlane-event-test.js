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
import { find, render, settled } from '@ember/test-helpers';
import { hbs } from 'ember-cli-htmlbars';

import Process from 'tez-ui/utils/process';
import Processor from 'tez-ui/utils/processor';

module('Integration | Component | em swimlane event', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {
    // default process color is #0 which is optimized away
    let red = {h: 0, s: 100, l: 50};
    this.set("process", Process.create({color: red}));
    this.set("processor", Processor.create());

    await render(hbs`<EmSwimlaneEvent @processor={{this.processor}} @process={{this.process}}/>`);

    // colors can't easily be compared as hsl is transformed to rgb
    assert.ok(find('.event-line').style.borderColor, "Should have borderColor");
    assert.ok(find('.event-bubble').style.borderColor, "Should have borderColor");

    // Template block usage:" + EOL +
    await render(hbs`<EmSwimlaneEvent @processor={{this.processor}} @process={{this.process}}>
        template block text
      </EmSwimlaneEvent>
    `);
    assert.ok(find('.event-line').style.borderColor, "Should have borderColor");
    assert.ok(find('.event-bubble').style.borderColor, "Should have borderColor");
  });

  test('Event position test', async function(assert) {
    this.set("process", Process.create());
    this.set("event", {
      time: 6
    });
    this.set("processor", Processor.create({
      startTime: 0,
      endTime: 10
    }));

    await render(hbs`<EmSwimlaneEvent @processor={{this.processor}} @process={{this.process}} @event={{this.event}}/>`);

    await settled();
    assert.dom('.em-swimlane-event').hasStyle({ left: '60%' });
  });
});
