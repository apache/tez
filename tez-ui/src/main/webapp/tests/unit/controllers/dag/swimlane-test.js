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

import EmberObject from '@ember/object';
import { setupTest } from 'ember-qunit';
import { module, test } from 'qunit';

module('Unit | Controller | dag/swimlane', function(hooks) {
  setupTest(hooks);

  test('Basic creation test', function(assert) {
    let controller = this.owner.factoryFor('controller:dag/swimlane').create({
      send() {},
      beforeSort: {bind() {}},
      initVisibleColumns() {},
      getCounterColumns: function () {
        return [];
      },
      model: []
    });

    assert.ok(controller);
    assert.ok(controller.zoom);
    assert.ok(controller.breadcrumbs);
    assert.ok(controller.columns);
    assert.equal(controller.columns.length, 13);
    assert.ok(controller.processes);

    assert.ok(controller.dataAvailable);

    assert.ok(controller.actions.toggleFullscreen);
  });

  test('Processes test', function(assert) {

    var vertices = [EmberObject.create({
      name: "v1",
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
    }), EmberObject.create({
      name: "v2"
    }), EmberObject.create({
      name: "v3"
    }), EmberObject.create({
      name: "v4"
    })];

    let controller = this.owner.factoryFor('controller:dag/swimlane').create({
      send() {},
      beforeSort: {bind() {}},
      initVisibleColumns() {},
      getCounterColumns: function () {
        return [];
      },
      model: vertices
    });

    var processes = controller.processes;

    assert.equal(processes[2].blockers[0].vertex, vertices[0]);
    assert.equal(processes[2].blockers[1].vertex, vertices[1]);
    assert.equal(processes[3].blockers[0].vertex, vertices[2]);
  });

  test('dataAvailable test', function(assert) {
    let controller = this.owner.factoryFor('controller:dag/swimlane').create({
      send() {},
      beforeSort: {bind() {}},
      initVisibleColumns() {},
      getCounterColumns: function () {
        return [];
      }
    }),
    dag = EmberObject.create(),
    vertex = EmberObject.create({
      dag: dag
    });

    assert.true(controller.dataAvailable, "No DAG or vertex");

    controller.set("model", EmberObject.create({
      firstObject: vertex
    }));
    assert.false(controller.dataAvailable, "With vertex & dag but no amWsVersion");

    dag.set("isComplete", true);
    assert.true(controller.dataAvailable, "Complete DAG");
    dag.set("isComplete", false);

    dag.set("amWsVersion", 1);
    assert.false(controller.dataAvailable, "With vertex & dag but amWsVersion=1");

    dag.set("amWsVersion", 2);
    assert.true(controller.dataAvailable, "With vertex & dag but amWsVersion=2");

    vertex.set("am", {});
    assert.false(controller.dataAvailable, "am loaded without event time data");

    vertex.set("am", {
      initTime: Date.now()
    });
    assert.true(controller.dataAvailable, "am loaded with event time data");
  });
});
