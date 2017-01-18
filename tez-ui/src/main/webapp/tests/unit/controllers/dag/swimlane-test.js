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

import Ember from 'ember';

import { moduleFor, test } from 'ember-qunit';

moduleFor('controller:dag/swimlane', 'Unit | Controller | dag/swimlane', {
  // Specify the other units that are required for this test.
  // needs: ['controller:foo']
});

test('Basic creation test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    beforeSort: {bind: Ember.K},
    initVisibleColumns: Ember.K,
    getCounterColumns: function () {
      return [];
    }
  });

  assert.ok(controller);
  assert.ok(controller.zoom);
  assert.ok(controller.breadcrumbs);
  assert.ok(controller.columns);
  assert.equal(controller.columns.length, 13);
  assert.ok(controller.processes);

  assert.ok(controller.dataAvailable);

  assert.ok(controller.actions.toggleFullscreen);
  assert.ok(controller.actions.click);
});

test('Processes test', function(assert) {

  var vertices = [Ember.Object.create({
    name: "v1"
  }), Ember.Object.create({
    name: "v2"
  }), Ember.Object.create({
    name: "v3"
  }), Ember.Object.create({
    name: "v4"
  })];
  vertices.firstObject = {
    dag: {
      edges: [{
        inputVertexName: "v1",
        outputVertexName: "v3"
      }, {
        inputVertexName: "v2",
        outputVertexName: "v3"
      }, {
        inputVertexName: "v3",
        outputVertexName: "v4"
      }]
    }
  };

  let controller = this.subject({
    send: Ember.K,
    beforeSort: {bind: Ember.K},
    initVisibleColumns: Ember.K,
    getCounterColumns: function () {
      return [];
    },
    model: vertices
  });

  var processes = controller.get("processes");

  assert.equal(processes[2].blockers[0].vertex, vertices[0]);
  assert.equal(processes[2].blockers[1].vertex, vertices[1]);
  assert.equal(processes[3].blockers[0].vertex, vertices[2]);
});

test('dataAvailable test', function(assert) {
  let controller = this.subject({
    send: Ember.K,
    beforeSort: {bind: Ember.K},
    initVisibleColumns: Ember.K,
    getCounterColumns: function () {
      return [];
    }
  }),
  dag = Ember.Object.create(),
  vertex = Ember.Object.create({
    dag: dag
  });

  assert.equal(controller.get("dataAvailable"), true, "No DAG or vertex");

  controller.set("model", Ember.Object.create({
    firstObject: vertex
  }));
  assert.equal(controller.get("dataAvailable"), false, "With vertex & dag but no amWsVersion");

  dag.set("isComplete", true);
  assert.equal(controller.get("dataAvailable"), true, "Complete DAG");
  dag.set("isComplete", false);

  dag.set("amWsVersion", 1);
  assert.equal(controller.get("dataAvailable"), false, "With vertex & dag but amWsVersion=1");

  dag.set("amWsVersion", 2);
  assert.equal(controller.get("dataAvailable"), true, "With vertex & dag but amWsVersion=2");

  vertex.set("am", {});
  assert.equal(controller.get("dataAvailable"), false, "am loaded without event time data");

  vertex.set("am", {
    initTime: Date.now()
  });
  assert.equal(controller.get("dataAvailable"), true, "am loaded with event time data");
});
