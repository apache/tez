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
import Processor from 'tez-ui/utils/processor';

moduleForComponent('em-swimlane-blocking-event', 'Integration | Component | em swimlane blocking event', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.set("process", Process.create());
  this.set("processor", Processor.create());

  this.render(hbs`{{em-swimlane-blocking-event processor=processor process=process}}`);

  assert.equal(this.$().text().trim(), '');

  // Template block usage:" + EOL +
  this.render(hbs`
    {{#em-swimlane-blocking-event processor=processor process=process}}
      template block text
    {{/em-swimlane-blocking-event}}
  `);

  assert.equal(this.$().text().trim(), '');
});

test('Blocking test', function(assert) {
  var blockingEventName = "blockingEvent",
      processIndex = 5,
      blockingIndex = 7,
      processColor = "#123456";

  this.set("process", Process.create({
    blockingEventName: blockingEventName,
    index: processIndex,
    getColor: function () {
      return processColor;
    },
    events: [{
      name: blockingEventName,
      time: 2
    }]
  }));
  this.set("blocking", Process.create({
    index: blockingIndex,
    endEvent: {
      time: 5
    }
  }));
  this.set("processor", Processor.create({
    startTime: 0,
    endTime: 10
  }));

  this.render(hbs`{{em-swimlane-blocking-event processor=processor process=process blocking=blocking}}`);

  return wait().then(() => {
    assert.equal(this.$(".em-swimlane-blocking-event").attr("style").trim(), 'left: 20%;');
    assert.equal(this.$(".event-line").css("height"), ((blockingIndex - processIndex) * 30) + "px");
  });
});

test('Blocking test with blocking.endEvent.time < blockTime', function(assert) {
  var blockingEventName = "blockingEvent",
      processIndex = 5,
      blockingIndex = 7,
      processColor = "#123456";

  this.set("process", Process.create({
    blockingEventName: blockingEventName,
    index: processIndex,
    getColor: function () {
      return processColor;
    },
    events: [{
      name: blockingEventName,
      time: 5
    }]
  }));
  this.set("blocking", Process.create({
    index: blockingIndex,
    endEvent: {
      time: 2
    }
  }));
  this.set("processor", Processor.create({
    startTime: 0,
    endTime: 10
  }));

  this.render(hbs`{{em-swimlane-blocking-event processor=processor process=process blocking=blocking}}`);

  return wait().then(() => {
    assert.equal(this.$(".em-swimlane-blocking-event").attr("style"), undefined);
  });
});
