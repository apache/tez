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
