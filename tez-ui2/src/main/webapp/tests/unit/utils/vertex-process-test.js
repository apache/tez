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

import VertexProcess from '../../../utils/vertex-process';
import { module, test } from 'qunit';

import Ember from 'ember';

module('Unit | Utility | vertex process');

test('Basic creation test', function(assert) {
  let process = VertexProcess.create();

  assert.ok(process);

  assert.ok(process.name);
  assert.ok(process.completeTime);
  assert.ok(process.blockingEventName);

  assert.ok(process.events);
  assert.ok(process.unblockTime);

  assert.ok(process.getTooltipContents);
});

test('unblockTime test', function(assert) {
  var process = VertexProcess.create();
  assert.equal(process.get("unblockTime"), undefined);

  process = VertexProcess.create({
    blockers: [VertexProcess.create({
      vertex: {
        endTime: 10
      }
    }), VertexProcess.create({
      vertex: {
        endTime: 15
      }
    }), VertexProcess.create({
      vertex: {
        endTime: 20
      }
    })]
  });

  assert.ok(process.get("unblockTime"), 20);

  process.blockers[2].set("vertex", Ember.Object.create({
    endTime: 12
  }));
  assert.ok(process.get("unblockTime"), 15);

  process.blockers[2].vertex.set("endTime", 25);
  assert.ok(process.get("unblockTime"), 25);
});

test('events test', function(assert) {
  var process = VertexProcess.create({
    vertex: Ember.Object.create({
      events: [{
        eventtype: "testEvent1"
      },{
        eventtype: "testEvent2"
      }],
      firstTaskStartTime: 10,
      lastTaskFinishTime: 20
    })
  });

  assert.equal(process.get("events.length"), 4);

  assert.equal(process.get("events.0.name"), "testEvent1");
  assert.equal(process.get("events.1.name"), "testEvent2");
  assert.equal(process.get("events.2.time"), 10);
  assert.equal(process.get("events.3.time"), 20);

  process.set("blockers", [VertexProcess.create({
    vertex: {
      endTime: 30
    }
  })]);

  assert.equal(process.get("events.length"), 5);
  assert.equal(process.get("events.4.time"), 30);
});

test('getTooltipContents-event test', function(assert) {
  var process = VertexProcess.create();

  var eventTooltip = process.getTooltipContents("event", {
    events: [{
      text: "TestEventText1",
      name: "TestEventName1",
      time: 10
    }, {
      text: "TestEventText2",
      name: "TestEventName2",
      time: 20
    }]
  });

  assert.equal(eventTooltip.length, 2);

  assert.equal(eventTooltip[0].title, "TestEventText1");
  assert.equal(eventTooltip[0].properties.length, 2);
  assert.equal(eventTooltip[0].properties[0].name, "Type");
  assert.equal(eventTooltip[0].properties[0].value, "TestEventName1");
  assert.equal(eventTooltip[0].properties[1].name, "Time");
  assert.equal(eventTooltip[0].properties[1].value, 10);

  assert.equal(eventTooltip[1].title, "TestEventText2");
  assert.equal(eventTooltip[1].properties.length, 2);
  assert.equal(eventTooltip[1].properties[0].name, "Type");
  assert.equal(eventTooltip[1].properties[0].value, "TestEventName2");
  assert.equal(eventTooltip[1].properties[1].name, "Time");
  assert.equal(eventTooltip[1].properties[1].value, 20);

});

test('getTooltipContents-process test', function(assert) {
  var process = VertexProcess.create({
    name: "TestName",
    vertex: Ember.Object.create({
      prop1: "val1",
      prop2: "val2",
      prop3: "val3"
    }),
    getVisibleProps: function () {
      return [Ember.Object.create({
        id: "prop1",
        headerTitle: "Prop 1",
        contentPath: "prop1"
      }), Ember.Object.create({
        id: "prop2",
        headerTitle: "Prop 2",
        contentPath: "prop2"
      })];
    }
  });

  var processTooltip = process.getTooltipContents("event-bar")[0];
  assert.equal(processTooltip.title, "TestName");

  assert.equal(processTooltip.properties.length, 2);

  assert.equal(processTooltip.properties[0].name, "Prop 1");
  assert.equal(processTooltip.properties[0].value, "val1");

  assert.equal(processTooltip.properties[1].name, "Prop 2");
  assert.equal(processTooltip.properties[1].value, "val2");

  processTooltip = process.getTooltipContents("process-line")[0];
  assert.equal(processTooltip.title, "TestName");

  assert.equal(processTooltip.properties.length, 2);

  assert.equal(processTooltip.properties[0].name, "Prop 1");
  assert.equal(processTooltip.properties[0].value, "val1");

  assert.equal(processTooltip.properties[1].name, "Prop 2");
  assert.equal(processTooltip.properties[1].value, "val2");

});
