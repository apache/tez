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
import Processor from 'tez-ui/utils/processor';

import wait from 'ember-test-helpers/wait';

moduleForComponent('em-swimlane-event', 'Integration | Component | em swimlane event', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.set("process", Process.create({}));
  this.set("processor", Processor.create());

  this.render(hbs`{{em-swimlane-event processor=processor process=process}}`);

  assert.ok(this.$(".event-bar"));
  assert.ok(this.$(".event-window"));

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-swimlane-event process=process processor=processor}}
      template block text
    {{/em-swimlane-event}}
  `);

  assert.ok(this.$(".event-bar"));
  assert.ok(this.$(".event-window"));
});

test('Event position test', function(assert) {
  this.set("process", Process.create());
  this.set("event", {
    time: 6
  });
  this.set("processor", Processor.create({
    startTime: 0,
    endTime: 10
  }));

  this.render(hbs`{{em-swimlane-event processor=processor process=process event=event}}`);

  return wait().then(() => {
    assert.equal(this.$(".em-swimlane-event").attr("style").trim(), "left: 60%;", "em-swimlane-event");
  });
});
